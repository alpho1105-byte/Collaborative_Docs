package editor

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"

	"collaborative-docs/internal/domain"
	"collaborative-docs/internal/ot"
	"collaborative-docs/internal/protocol"
	"collaborative-docs/internal/store"
)

var (
	errMoved = errors.New("document moved to another node")
	errFull  = errors.New("document has reached its editor limit")
)

type (
	joinMsg struct {
		c     *conn
		rev   int64
		reply chan error
	}
	leaveMsg struct{ c *conn }
	editMsg  struct {
		c         *conn
		cseq, rev int64
		op        ot.Op
	}
	cursorMsg struct {
		c   *conn
		rev int64
		pos int
	}
	handoffMsg struct{}
	stopMsg    struct{}
)

type cursorState struct {
	userID string
	pos    int
}

// session 是一份文件在 owner node 上的唯一序列器。所有狀態只由 run 這個 goroutine 存取：
// 編輯依到達順序轉換、指派 seq、交給 store 寫入；寫入 durable 之後才 ack 編輯者並廣播給其他人。
type session struct {
	n       *Node
	docID   string
	inbox   chan any
	results resultQueue
	ready   chan struct{}
	done    chan struct{}
	loadErr error

	epoch int64
	// text 是 durable 版本的全文。headLen 是含寫入中編輯的最新長度，轉換新編輯只需要長度與 window。
	text    []rune
	durable int64
	head    int64
	headLen int
	// window 保留最近的 change（包含還在寫入的），用來轉換基於舊版本送來的編輯。
	window []domain.Change
	// applied 包含寫入中的編輯，用來擋下重送；durableMarks 只含已經 durable 的，寫進 snapshot 與 welcome。
	applied      map[string]domain.ClientMark
	durableMarks map[string]domain.ClientMark
	editors      map[*conn]struct{}
	byClient     map[string]*conn
	waiting      map[int64]*conn
	cursors      map[string]cursorState
	draining     bool
}

func newSession(n *Node, docID string) *session {
	return &session{n: n, docID: docID, inbox: make(chan any, 1024), results: resultQueue{notify: make(chan struct{}, 1)},
		ready: make(chan struct{}), done: make(chan struct{}),
		editors: make(map[*conn]struct{}), byClient: make(map[string]*conn), waiting: make(map[int64]*conn), cursors: make(map[string]cursorState)}
}

func (s *session) post(m any) bool {
	select {
	case s.inbox <- m:
		return true
	case <-s.done:
		return false
	}
}

func (s *session) join(c *conn, rev int64) error {
	reply := make(chan error, 1)
	if !s.post(joinMsg{c: c, rev: rev, reply: reply}) {
		return errMoved
	}
	select {
	case err := <-reply:
		return err
	case <-s.done:
		return errMoved
	}
}

func (s *session) run() {
	defer close(s.done)
	defer s.n.remove(s)
	if err := s.load(); err != nil {
		s.loadErr = err
		close(s.ready)
		return
	}
	close(s.ready)
	for {
		select {
		case <-s.n.ctx.Done():
			return
		case m := <-s.inbox:
			if !s.handle(m) {
				return
			}
		case <-s.results.notify:
			if !s.onDurable() {
				return
			}
		}
		if s.draining && s.durable == s.head {
			s.finishHandoff()
			return
		}
	}
}

// load 先 claim 再讀取：claim 讓舊 owner 之後的寫入全部失敗，claim 之前已經 commit 的 change 則一定讀得到。
func (s *session) load() error {
	ctx := s.n.ctx
	epoch, err := s.n.d.Store.Claim(ctx, s.docID, s.n.id)
	if err != nil {
		return err
	}
	snap, changes, err := s.n.d.Store.Load(ctx, s.docID)
	if err != nil {
		return err
	}
	s.epoch = epoch
	s.text = []rune(snap.Text)
	s.durableMarks = maps.Clone(snap.Clients)
	if s.durableMarks == nil {
		s.durableMarks = make(map[string]domain.ClientMark)
	}
	s.durable = snap.Seq
	for _, c := range changes {
		if s.text, err = c.Op.Apply(s.text); err != nil {
			return err
		}
		s.durable = c.Seq
		s.durableMarks[c.ClientID] = domain.ClientMark{ClientSeq: c.ClientSeq, Seq: c.Seq}
	}
	s.head, s.headLen = s.durable, len(s.text)
	s.window = changes[max(0, len(changes)-s.n.cfg.Window):]
	s.applied = maps.Clone(s.durableMarks)
	s.n.stats.Loads.Add(1)
	s.n.stats.Replayed.Add(int64(len(changes)))
	return nil
}

func (s *session) handle(m any) bool {
	switch m := m.(type) {
	case joinMsg:
		m.reply <- s.onJoin(m.c, m.rev)
	case leaveMsg:
		s.onLeave(m.c)
	case editMsg:
		s.onEdit(m)
	case cursorMsg:
		s.onCursor(m)
	case handoffMsg:
		s.draining = true
	case stopMsg:
		s.closeAll(movedFrame())
		return false
	}
	return true
}

// oldestRev 是還能轉換的最舊版本：基於更舊版本的編輯需要 client 重新同步。
func (s *session) oldestRev() int64 {
	if len(s.window) == 0 {
		return s.head
	}
	return s.window[0].Seq - 1
}

func (s *session) changesAfter(rev int64) []domain.Change {
	if len(s.window) == 0 {
		return nil
	}
	return s.window[rev+1-s.window[0].Seq:]
}

func (s *session) onJoin(c *conn, rev int64) error {
	if s.draining {
		return errMoved
	}
	if old := s.byClient[c.clientID]; old != nil {
		// 同一個 client 重連時舊連線可能還沒斷，以新的為準。
		s.onLeave(old)
		old.finish(movedFrame())
	}
	if len(s.editors) >= s.n.cfg.MaxEditors {
		return errFull
	}
	f := protocol.ServerFrame{Type: protocol.TypeWelcome, DocID: s.docID, Seq: s.durable, AckedCSeq: s.durableMarks[c.clientID].ClientSeq}
	if changes, ok := s.catchUp(rev); ok {
		f.Changes = changes
	} else {
		text := string(s.text)
		f.Text = &text
	}
	for id, cur := range s.cursors {
		f.Cursors = append(f.Cursors, protocol.Cursor{ClientID: id, UserID: cur.userID, Position: cur.pos})
	}
	c.send(f)
	s.editors[c] = struct{}{}
	s.byClient[c.clientID] = c
	return nil
}

// catchUp 回傳 client 從 rev 到目前 durable 版本缺少的 change。太舊而被 compaction 清掉時回傳 false，改送全文。
func (s *session) catchUp(rev int64) ([]protocol.Change, bool) {
	if rev < 0 || rev > s.durable {
		return nil, false
	}
	var changes []domain.Change
	if rev < s.oldestRev() {
		older, err := s.n.d.Store.ChangesSince(s.n.ctx, s.docID, rev, s.oldestRev())
		if err != nil {
			return nil, false
		}
		changes = older
		rev = s.oldestRev()
	}
	for _, c := range s.changesAfter(rev) {
		if c.Seq > s.durable {
			break
		}
		changes = append(changes, c)
	}
	out := make([]protocol.Change, len(changes))
	for i, c := range changes {
		out[i] = toFrame(c)
	}
	return out, true
}

func (s *session) onLeave(c *conn) {
	if _, ok := s.editors[c]; !ok {
		return
	}
	delete(s.editors, c)
	if s.byClient[c.clientID] == c {
		delete(s.byClient, c.clientID)
		delete(s.cursors, c.clientID)
		s.broadcast(protocol.ServerFrame{Type: protocol.TypeLeft, ClientID: c.clientID}, nil)
	}
}

func (s *session) onEdit(m editMsg) {
	if _, ok := s.editors[m.c]; !ok || s.draining {
		// 交接中不再接受編輯：client 還沒收到 ack，會在連上新的 owner 後重送。
		return
	}
	clientID := m.c.clientID
	if mark, ok := s.applied[clientID]; ok && m.cseq <= mark.ClientSeq {
		// 斷線重連後重送的編輯已經套用過，只回 ack，不能再套用一次。
		s.n.stats.Duplicates.Add(1)
		if m.cseq == mark.ClientSeq && mark.Seq <= s.durable {
			m.c.send(protocol.ServerFrame{Type: protocol.TypeAck, CSeq: m.cseq, Seq: mark.Seq})
		}
		return
	}
	if m.rev < s.oldestRev() || m.rev > s.durable {
		m.c.send(errorFrame(protocol.CodeStale, "resync required"))
		return
	}
	op := m.op
	var err error
	for _, c := range s.changesAfter(m.rev) {
		if op, _, err = ot.Transform(op, c.Op); err != nil {
			break
		}
	}
	if err != nil || op.BaseLen() != s.headLen {
		m.c.finish(errorFrame(protocol.CodeBadEdit, "edit does not match the document at its revision"))
		return
	}
	s.head++
	s.headLen = op.TargetLen()
	c := domain.Change{DocID: s.docID, Seq: s.head, ClientID: clientID, ClientSeq: m.cseq, UserID: m.c.userID, Op: op, At: time.Now()}
	s.window = append(s.window, c)
	s.applied[clientID] = domain.ClientMark{ClientSeq: m.cseq, Seq: c.Seq}
	s.waiting[c.Seq] = m.c
	seq := c.Seq
	s.n.d.Store.AppendAsync(s.docID, s.epoch, []domain.Change{c}, func(err error) {
		s.results.push(appendResult{seq: seq, err: err})
	})
}

func (s *session) onDurable() bool {
	for _, r := range s.results.take() {
		if r.err == nil && r.seq != s.durable+1 {
			r.err = errors.New("writes became durable out of order")
		}
		if r.err != nil {
			s.fail(r.err)
			return false
		}
		c := s.window[r.seq-s.window[0].Seq]
		text, err := c.Op.Apply(s.text)
		if err != nil {
			s.fail(err)
			return false
		}
		s.text, s.durable = text, r.seq
		s.durableMarks[c.ClientID] = domain.ClientMark{ClientSeq: c.ClientSeq, Seq: c.Seq}
		for id, cur := range s.cursors {
			cur.pos = ot.TransformIndex(cur.pos, c.Op, id == c.ClientID)
			s.cursors[id] = cur
		}
		sender := s.waiting[r.seq]
		delete(s.waiting, r.seq)
		if sender != nil {
			sender.send(protocol.ServerFrame{Type: protocol.TypeAck, CSeq: c.ClientSeq, Seq: c.Seq})
		}
		op := c.Op
		s.broadcast(protocol.ServerFrame{Type: protocol.TypeOp, Seq: c.Seq, ClientID: c.ClientID, CSeq: c.ClientSeq, UserID: c.UserID, Op: &op}, sender)
		s.n.d.Publisher.Publish(c)
		s.n.stats.Applied.Add(1)
	}
	s.trim()
	return true
}

func (s *session) onCursor(m cursorMsg) {
	if _, ok := s.editors[m.c]; !ok || m.rev < s.oldestRev() || m.rev > s.durable {
		return
	}
	pos := m.pos
	for _, c := range s.changesAfter(m.rev) {
		if c.Seq > s.durable {
			break
		}
		pos = ot.TransformIndex(pos, c.Op, c.ClientID == m.c.clientID)
	}
	pos = max(0, min(pos, len(s.text)))
	s.cursors[m.c.clientID] = cursorState{userID: m.c.userID, pos: pos}
	s.broadcast(protocol.ServerFrame{Type: protocol.TypeCursors, ClientID: m.c.clientID, UserID: m.c.userID, Position: pos}, m.c)
}

func (s *session) broadcast(f protocol.ServerFrame, except *conn) {
	for c := range s.editors {
		if c != except {
			c.send(f)
		}
	}
}

// fail 處理寫入失敗：通常是被 fence（文件已經交給新的 owner）。還沒 durable 的編輯都沒有 ack 過，
// 讓所有編輯者去找新的 owner 重送即可。
func (s *session) fail(err error) {
	if errors.Is(err, store.ErrFenced) {
		s.n.stats.Fenced.Add(1)
		s.n.d.Log.Info("lost ownership", "doc", s.docID, "epoch", s.epoch)
	} else {
		s.n.d.Log.Error("append failed", "doc", s.docID, "err", err)
	}
	s.closeAll(movedFrame())
}

// finishHandoff 在所有寫入都 durable 之後寫一份 snapshot，新 owner 載入時就不用重播這段期間的 change。
func (s *session) finishHandoff() {
	snap := domain.Snapshot{DocID: s.docID, Seq: s.durable, Text: string(s.text), Clients: s.durableMarks}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.n.d.Store.SaveSnapshot(ctx, snap); err != nil {
		s.n.d.Log.Error("handoff snapshot failed", "doc", s.docID, "err", err)
	}
	s.n.stats.Handoffs.Add(1)
	s.closeAll(movedFrame())
}

func (s *session) closeAll(f protocol.ServerFrame) {
	for c := range s.editors {
		c.finish(f)
	}
}

func (s *session) trim() {
	if len(s.window) <= 2*s.n.cfg.Window {
		return
	}
	cut := len(s.window) - s.n.cfg.Window
	for cut > 0 && s.window[cut-1].Seq > s.durable {
		cut--
	}
	s.window = slices.Clone(s.window[cut:])
}

func toFrame(c domain.Change) protocol.Change {
	return protocol.Change{Seq: c.Seq, ClientID: c.ClientID, CSeq: c.ClientSeq, UserID: c.UserID, Op: c.Op}
}

type appendResult struct {
	seq int64
	err error
}

// resultQueue 接收 store 的寫入結果。store 依排入順序回呼，所以結果的順序就是 seq 的順序。
type resultQueue struct {
	mu     sync.Mutex
	items  []appendResult
	notify chan struct{}
}

func (q *resultQueue) push(r appendResult) {
	q.mu.Lock()
	q.items = append(q.items, r)
	q.mu.Unlock()
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *resultQueue) take() []appendResult {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := q.items
	q.items = nil
	return out
}
