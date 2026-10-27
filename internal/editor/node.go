// Package editor 是 Document Service 的編輯節點。每份文件由 consistent hashing 決定唯一的 owner node，
// 所有編輯者都連到 owner，由它定序、轉換、寫入 operation store，再廣播給其他編輯者。
package editor

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"collaborative-docs/internal/auth"
	"collaborative-docs/internal/docs"
	"collaborative-docs/internal/domain"
	"collaborative-docs/internal/protocol"
	"collaborative-docs/internal/registry"
	"collaborative-docs/internal/ring"
	"collaborative-docs/internal/store"
)

type Config struct {
	// MaxEditors 是單一文件同時編輯的人數上限。
	MaxEditors int
	// Window 是 session 在記憶體保留的最近 change 數；基於更舊版本的編輯要求 client 重新同步。
	Window     int
	SendBuffer int
	Heartbeat  time.Duration
	VNodes     int
}

func DefaultConfig() Config {
	return Config{MaxEditors: 100, Window: 1024, SendBuffer: 1024, Heartbeat: 200 * time.Millisecond, VNodes: 128}
}

// Publisher 把 durable 的 change 發佈到 durable event queue，給 viewer 叢集消費。
type Publisher interface {
	Publish(domain.Change)
}

type Deps struct {
	Store     *store.Store
	Docs      *docs.Service
	Registry  *registry.Registry
	Signer    *auth.Signer
	Publisher Publisher
	Log       *slog.Logger
}

type Stats struct {
	Applied    atomic.Int64
	Duplicates atomic.Int64
	Fenced     atomic.Int64
	Handoffs   atomic.Int64
	Loads      atomic.Int64
	Replayed   atomic.Int64
}

type Node struct {
	id    string
	cfg   Config
	d     Deps
	stats Stats
	ring  atomic.Pointer[ring.Ring]

	ctx    context.Context
	cancel context.CancelFunc
	srv    *http.Server
	addr   string
	paused atomic.Bool

	mu       sync.Mutex
	sessions map[string]*session
	conns    map[*conn]struct{}
}

func New(id string, cfg Config, d Deps) *Node {
	return &Node{id: id, cfg: cfg, d: d, sessions: make(map[string]*session), conns: make(map[*conn]struct{})}
}

func (n *Node) ID() string    { return n.id }
func (n *Node) Stats() *Stats { return &n.stats }

// Start 在 ln 上提供 WebSocket，並以 advertise 這個位址註冊到 registry（空字串時用 ln 的位址）。
func (n *Node) Start(ctx context.Context, ln net.Listener, advertise string) {
	n.ctx, n.cancel = context.WithCancel(ctx)
	n.addr = advertise
	if n.addr == "" {
		n.addr = ln.Addr().String()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws/docs/{id}", n.handleWS)
	n.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go n.srv.Serve(ln)
	n.d.Registry.Register(n.registration())
	go n.heartbeatLoop()
	go n.watchLoop()
}

func (n *Node) registration() registry.Node {
	return registry.Node{ID: n.id, Kind: registry.KindEdit, Addr: n.addr}
}

func (n *Node) heartbeatLoop() {
	t := time.NewTicker(n.cfg.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
		}
		if n.paused.Load() {
			continue
		}
		if !n.d.Registry.Heartbeat(n.id) {
			// 租約過期：這段期間文件可能已經交給別人，先全部放下再重新註冊。
			n.d.Log.Warn("lease expired, stepping down", "node", n.id)
			n.stepDown()
			n.d.Registry.Register(n.registration())
		}
	}
}

func (n *Node) watchLoop() {
	ch := n.d.Registry.Watch(registry.KindEdit)
	for {
		select {
		case <-n.ctx.Done():
			return
		case m := <-ch:
			if !n.paused.Load() {
				n.setMembership(m)
			}
		}
	}
}

// setMembership 更新本地的 hash ring，把不再屬於自己的文件交接出去。
func (n *Node) setMembership(m registry.Membership) {
	r := ring.New(m.IDs(), n.cfg.VNodes)
	n.ring.Store(r)
	for _, s := range n.liveSessions() {
		if owner, _ := r.Owner(s.docID); owner != n.id {
			s.post(handoffMsg{})
		}
	}
}

func (n *Node) owns(docID string) bool {
	owner, ok := n.ring.Load().Owner(docID)
	return ok && owner == n.id
}

// Docs 回傳目前在這台 node 上有 session 的文件。
func (n *Node) Docs() []string {
	var out []string
	for _, s := range n.liveSessions() {
		out = append(out, s.docID)
	}
	return out
}

func (n *Node) liveSessions() []*session {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]*session, 0, len(n.sessions))
	for _, s := range n.sessions {
		out = append(out, s)
	}
	return out
}

func (n *Node) session(docID string) (*session, error) {
	n.mu.Lock()
	s, ok := n.sessions[docID]
	if !ok {
		s = newSession(n, docID)
		n.sessions[docID] = s
		go s.run()
	}
	n.mu.Unlock()
	select {
	case <-s.ready:
	case <-n.ctx.Done():
		return nil, n.ctx.Err()
	}
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	return s, nil
}

func (n *Node) remove(s *session) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sessions[s.docID] == s {
		delete(n.sessions, s.docID)
	}
}

func (n *Node) stepDown() {
	for _, s := range n.liveSessions() {
		s.post(stopMsg{})
	}
}

// handleWS 在 upgrade 之前驗證身分與編輯權限。瀏覽器的 WebSocket API 不能自訂 header，所以接受 query string 的 token。
func (n *Node) handleWS(w http.ResponseWriter, r *http.Request) {
	docID := r.PathValue("id")
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		token = r.URL.Query().Get("token")
	}
	user, err := n.d.Signer.Verify(token)
	if err != nil {
		http.Error(w, "invalid or missing token", http.StatusUnauthorized)
		return
	}
	if role, err := n.d.Docs.Role(r.Context(), docID, user); err != nil || !role.CanEdit() {
		http.Error(w, "no edit access", http.StatusForbidden)
		return
	}
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	ws.SetReadLimit(256 << 10)
	c := newConn(ws, user, n.cfg.SendBuffer)
	if !n.track(c) {
		ws.CloseNow()
		return
	}
	defer n.untrack(c)
	go c.writeLoop(n.ctx)

	var f protocol.ClientFrame
	if err := wsjson.Read(n.ctx, ws, &f); err != nil || f.Type != protocol.TypeJoin || f.ClientID == "" {
		c.finish(errorFrame(protocol.CodeBadEdit, "the first frame must be a join with a client_id"))
		return
	}
	c.clientID = f.ClientID
	if !n.owns(docID) {
		c.finish(movedFrame())
		return
	}
	s, err := n.session(docID)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			c.finish(errorFrame(protocol.CodeForbidden, "document not found"))
		} else {
			c.finish(movedFrame())
		}
		return
	}
	switch err := s.join(c, f.Rev); {
	case errors.Is(err, errFull):
		c.finish(errorFrame(protocol.CodeDocFull, err.Error()))
		return
	case err != nil:
		c.finish(movedFrame())
		return
	}
	defer s.post(leaveMsg{c: c})
	c.readLoop(n.ctx, s)
}

func (n *Node) track(c *conn) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ctx.Err() != nil {
		return false
	}
	n.conns[c] = struct{}{}
	return true
}

func (n *Node) untrack(c *conn) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.conns, c)
}

// Kill 模擬 node 當機：不交接、不註銷，連線直接斷掉，租約要等過期才會被 registry 移除。
func (n *Node) Kill() {
	n.cancel()
	n.srv.Close()
	n.mu.Lock()
	defer n.mu.Unlock()
	for c := range n.conns {
		c.ws.CloseNow()
	}
}

// Pause 模擬 node 和 registry 之間的網路分區（或長時間 GC）：停止 heartbeat、收不到名單更新，
// 但仍然服務已經連上的編輯者。期間文件可能被交給別人，它之後的寫入會被 fence。
func (n *Node) Pause(d time.Duration) {
	n.paused.Store(true)
	time.AfterFunc(d, func() {
		n.paused.Store(false)
		n.setMembership(n.d.Registry.Members(registry.KindEdit))
	})
}

// Shutdown 正常下線：先註銷，把所有文件交接出去，等交接完成再關閉。
func (n *Node) Shutdown(ctx context.Context) {
	n.d.Registry.Deregister(n.id)
	n.setMembership(n.d.Registry.Members(registry.KindEdit))
	for len(n.liveSessions()) > 0 && ctx.Err() == nil {
		time.Sleep(20 * time.Millisecond)
	}
	n.Kill()
}
