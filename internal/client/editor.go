package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"collaborative-docs/internal/metrics"
	"collaborative-docs/internal/ot"
	"collaborative-docs/internal/protocol"
)

var (
	ErrDocFull  = errors.New("document has reached its editor limit")
	ErrNoAccess = errors.New("no edit access to the document")
	errMoved    = errors.New("document moved to another node")
)

type EditorConfig struct {
	DocID       string
	ClientID    string
	CursorEvery time.Duration
	// OnRemote 在收到其他人的即時編輯時呼叫（不持有鎖），模擬器用它量測廣播延遲。
	OnRemote func(op ot.Op)
	// AckLatency 收集送出到收到 ack 的時間，只量同一條連線上的編輯，不含斷線重送；可以多個 editor 共用。
	AckLatency *metrics.Latency
}

type EditorStats struct {
	Sent       atomic.Int64
	Resent     atomic.Int64
	Acked      atomic.Int64
	Reconnects atomic.Int64
	Discarded  atomic.Int64
}

type sentInfo struct {
	at  time.Time
	gen int64
}

// Editor 是一個編輯者：本地編輯立即生效，背景維持到文件 owner 的 WebSocket，斷線就問 gateway 重新連線。
type Editor struct {
	api   *API
	cfg   EditorConfig
	stats EditorStats

	mu   sync.Mutex
	doc  *Doc
	out  chan protocol.ClientFrame
	drop context.CancelFunc
	gen  int64
	sent map[int64]sentInfo

	cancel context.CancelFunc
	done   chan struct{}
}

func NewEditor(api *API, cfg EditorConfig) *Editor {
	if cfg.ClientID == "" {
		cfg.ClientID = fmt.Sprintf("cl_%016x", rand.Uint64())
	}
	if cfg.CursorEvery == 0 {
		cfg.CursorEvery = 100 * time.Millisecond
	}
	return &Editor{api: api, cfg: cfg, doc: NewDoc(cfg.ClientID), sent: make(map[int64]sentInfo)}
}

func (e *Editor) Stats() *EditorStats { return &e.stats }
func (e *Editor) ClientID() string    { return e.cfg.ClientID }

// Start 連線並等到第一個 welcome，之後在背景維持連線。
func (e *Editor) Start(ctx context.Context) error {
	ctx, e.cancel = context.WithCancel(ctx)
	e.done = make(chan struct{})
	first := make(chan error, 1)
	go e.run(ctx, first)
	select {
	case err := <-first:
		if err != nil {
			e.Close()
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Editor) run(ctx context.Context, first chan<- error) {
	defer close(e.done)
	backoff := 20 * time.Millisecond
	started := false
	for ctx.Err() == nil {
		welcomed, err := e.connect(ctx, func() {
			if !started {
				started = true
				first <- nil
			}
		})
		if !started && (errors.Is(err, ErrDocFull) || errors.Is(err, ErrNoAccess)) {
			first <- err
			return
		}
		e.mu.Lock()
		e.doc.SetOffline()
		e.out = nil
		e.mu.Unlock()
		if welcomed {
			backoff = 20 * time.Millisecond
		} else {
			backoff = min(2*backoff, time.Second)
		}
		e.stats.Reconnects.Add(1)
		select {
		case <-ctx.Done():
		case <-time.After(backoff/2 + rand.N(backoff/2)):
		}
	}
}

func (e *Editor) connect(ctx context.Context, onWelcome func()) (bool, error) {
	wsURL, err := e.api.EditURL(ctx, e.cfg.DocID)
	if err != nil {
		var he *HTTPError
		if errors.As(err, &he) && (he.Status == 403 || he.Status == 404) {
			return false, ErrNoAccess
		}
		return false, err
	}
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	ws, _, err := websocket.Dial(connCtx, wsURL+"?token="+url.QueryEscape(e.api.Token), nil)
	if err != nil {
		return false, err
	}
	defer ws.CloseNow()
	ws.SetReadLimit(64 << 20)

	e.mu.Lock()
	join := protocol.ClientFrame{Type: protocol.TypeJoin, ClientID: e.cfg.ClientID, Rev: e.doc.Rev()}
	e.mu.Unlock()
	if err := wsjson.Write(connCtx, ws, join); err != nil {
		return false, err
	}
	var f protocol.ServerFrame
	if err := wsjson.Read(connCtx, ws, &f); err != nil {
		return false, err
	}
	switch {
	case f.Type == protocol.TypeError && f.Code == protocol.CodeDocFull:
		return false, ErrDocFull
	case f.Type == protocol.TypeError && f.Code == protocol.CodeForbidden:
		return false, ErrNoAccess
	case f.Type != protocol.TypeWelcome:
		return false, errMoved
	}

	out := make(chan protocol.ClientFrame, 4096)
	e.mu.Lock()
	resend, acked, discarded, err := e.doc.Welcome(f)
	if err != nil {
		e.mu.Unlock()
		return false, err
	}
	e.gen++
	e.out, e.drop = out, cancel
	if discarded {
		e.stats.Discarded.Add(1)
	}
	e.acked(acked)
	if resend != nil {
		e.sendEdit(resend)
	}
	e.mu.Unlock()
	onWelcome()

	go e.writeLoop(connCtx, ws, out, cancel)
	go e.cursorLoop(connCtx)
	return true, e.readLoop(connCtx, ws)
}

func (e *Editor) writeLoop(ctx context.Context, ws *websocket.Conn, out <-chan protocol.ClientFrame, cancel context.CancelFunc) {
	for {
		select {
		case <-ctx.Done():
			return
		case f := <-out:
			if err := wsjson.Write(ctx, ws, f); err != nil {
				cancel()
				return
			}
		}
	}
}

// cursorLoop 每隔 CursorEvery 回報一次游標（有變動、而且沒有待確認的編輯時）。
func (e *Editor) cursorLoop(ctx context.Context) {
	t := time.NewTicker(e.cfg.CursorEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.mu.Lock()
			if f := e.doc.CursorFrame(); f != nil {
				e.push(*f)
			}
			e.mu.Unlock()
		}
	}
}

func (e *Editor) readLoop(ctx context.Context, ws *websocket.Conn) error {
	for {
		var f protocol.ServerFrame
		if err := wsjson.Read(ctx, ws, &f); err != nil {
			return err
		}
		remote, err := e.handle(f)
		if err != nil {
			return err
		}
		if remote != nil && e.cfg.OnRemote != nil {
			e.cfg.OnRemote(*remote)
		}
	}
}

func (e *Editor) handle(f protocol.ServerFrame) (*ot.Op, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	var remote *ot.Op
	switch f.Type {
	case protocol.TypeAck:
		acked, err := e.doc.Ack(f.CSeq, f.Seq)
		if err != nil {
			return nil, err
		}
		e.acked(acked)
	case protocol.TypeOp:
		if f.Op == nil {
			return nil, fmt.Errorf("%w: op frame without an op", ErrOutOfSync)
		}
		acked, err := e.doc.Apply(protocol.Change{Seq: f.Seq, ClientID: f.ClientID, CSeq: f.CSeq, UserID: f.UserID, Op: *f.Op})
		if err != nil {
			return nil, err
		}
		e.acked(acked)
		if f.ClientID != e.cfg.ClientID {
			remote = f.Op
		}
	case protocol.TypeCursors:
		e.doc.SetRemoteCursor(protocol.Cursor{ClientID: f.ClientID, UserID: f.UserID, Position: f.Position})
	case protocol.TypeLeft:
		e.doc.RemoveRemote(f.ClientID)
	case protocol.TypeMoved:
		return nil, errMoved
	case protocol.TypeError:
		return nil, fmt.Errorf("server error %s: %s", f.Code, f.Message)
	}
	e.pump()
	return remote, nil
}

func (e *Editor) pump() {
	if f := e.doc.Next(); f != nil {
		e.sendEdit(f)
	}
}

func (e *Editor) sendEdit(f *protocol.ClientFrame) {
	if info, ok := e.sent[f.CSeq]; ok {
		info.gen = -1
		e.sent[f.CSeq] = info
		e.stats.Resent.Add(1)
	} else {
		e.sent[f.CSeq] = sentInfo{at: time.Now(), gen: e.gen}
		e.stats.Sent.Add(1)
	}
	e.push(*f)
}

func (e *Editor) push(f protocol.ClientFrame) {
	if e.out == nil {
		return
	}
	select {
	case e.out <- f:
	default:
		e.drop()
	}
}

func (e *Editor) acked(cseq int64) {
	if cseq == 0 {
		return
	}
	info, ok := e.sent[cseq]
	delete(e.sent, cseq)
	e.stats.Acked.Add(1)
	if ok && info.gen == e.gen {
		e.cfg.AckLatency.Observe(time.Since(info.at))
	}
}

// Update 在鎖內執行 fn：fn 讀取目前的文件並做本地編輯，編輯在 fn 結束後送出。
func (e *Editor) Update(fn func(d *Doc) error) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	err := fn(e.doc)
	e.pump()
	return err
}

func (e *Editor) Insert(pos int, s string) error {
	return e.Update(func(d *Doc) error { return d.Insert(pos, s) })
}

func (e *Editor) Delete(pos, n int) error {
	return e.Update(func(d *Doc) error { return d.Delete(pos, n) })
}

// Read 在鎖內讓 fn 讀取文件狀態。
func (e *Editor) Read(fn func(d *Doc)) {
	e.mu.Lock()
	defer e.mu.Unlock()
	fn(e.doc)
}

// DropConnection 模擬網路中斷，client 會自動重連。
func (e *Editor) DropConnection() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.drop != nil {
		e.drop()
	}
}

func (e *Editor) Close() {
	e.cancel()
	<-e.done
}
