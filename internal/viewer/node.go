// Package viewer 是 viewer mode 專用的 Document Service 叢集。
//
// viewer 不會送出編輯，所以不必連到文件的 edit owner：edit owner 把 durable 的 change 發佈到
// event queue（依 doc id 分區），每個分區由一台 viewer node 消費，再用 SSE 單向推送給觀看者。
// viewer 只需要依 seq 套用，不做 OT。
package viewer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"collaborative-docs/internal/auth"
	"collaborative-docs/internal/docs"
	"collaborative-docs/internal/domain"
	"collaborative-docs/internal/eventbus"
	"collaborative-docs/internal/registry"
	"collaborative-docs/internal/ring"
	"collaborative-docs/internal/store"
)

var errMoved = errors.New("partition is owned by another viewer node")

type Config struct {
	// Tail 是每份文件保留的最近 change 數，重連的 viewer 落後不超過這個數量時只補差異。
	Tail int
	// Reconcile：多久沒收到某份文件的 change 就向 operation store 確認一次最新版本。
	// edit owner 在寫入之後、發佈之前當機時，這是 viewer 補上那個 change 的唯一途徑。
	Reconcile  time.Duration
	SendBuffer int
	Heartbeat  time.Duration
	VNodes     int
}

func DefaultConfig() Config {
	return Config{Tail: 512, Reconcile: time.Second, SendBuffer: 1024, Heartbeat: 200 * time.Millisecond, VNodes: 128}
}

// PartitionKey 是分區在 hash ring 上的 key。gateway 用它把 viewer 導到負責該分區的 node。
func PartitionKey(p int) string {
	return "partition-" + strconv.Itoa(p)
}

type Deps struct {
	Store    *store.Store
	Docs     *docs.Service
	Registry *registry.Registry
	Signer   *auth.Signer
	Bus      *eventbus.Topic[domain.Change]
	Log      *slog.Logger
}

type Stats struct {
	Delivered  atomic.Int64
	Backfilled atomic.Int64
	Resumed    atomic.Int64
	Snapshots  atomic.Int64
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

	mu    sync.Mutex
	parts map[int]context.CancelFunc
	docs  map[string]*docState
}

func New(id string, cfg Config, d Deps) *Node {
	return &Node{id: id, cfg: cfg, d: d, parts: make(map[int]context.CancelFunc), docs: make(map[string]*docState)}
}

func (n *Node) ID() string    { return n.id }
func (n *Node) Stats() *Stats { return &n.stats }

func (n *Node) Start(ctx context.Context, ln net.Listener, advertise string) {
	n.ctx, n.cancel = context.WithCancel(ctx)
	n.addr = advertise
	if n.addr == "" {
		n.addr = ln.Addr().String()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /sse/docs/{id}", n.handleSSE)
	n.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go n.srv.Serve(ln)
	reg := registry.Node{ID: n.id, Kind: registry.KindView, Addr: n.addr}
	n.d.Registry.Register(reg)
	go n.heartbeatLoop(reg)
	go n.watchLoop()
	go n.reconcileLoop()
}

func (n *Node) heartbeatLoop(reg registry.Node) {
	t := time.NewTicker(n.cfg.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
			if !n.d.Registry.Heartbeat(n.id) {
				n.d.Registry.Register(reg)
			}
		}
	}
}

func (n *Node) watchLoop() {
	ch := n.d.Registry.Watch(registry.KindView)
	for {
		select {
		case <-n.ctx.Done():
			return
		case m := <-ch:
			n.setMembership(m)
		}
	}
}

// setMembership 依新的 hash ring 調整負責的分區：新分區從目前的結尾開始消費（文件狀態在有人訂閱時
// 從 operation store 載入），失去的分區停止消費並斷開觀看者，讓他們透過 gateway 找新的 node。
func (n *Node) setMembership(m registry.Membership) {
	r := ring.New(m.IDs(), n.cfg.VNodes)
	n.ring.Store(r)
	n.mu.Lock()
	defer n.mu.Unlock()
	for p := range n.d.Bus.Partitions() {
		owner, _ := r.Owner(PartitionKey(p))
		cancel, running := n.parts[p]
		switch {
		case owner == n.id && !running:
			ctx, cancel := context.WithCancel(n.ctx)
			n.parts[p] = cancel
			go n.consume(ctx, p, n.d.Bus.End(p))
		case owner != n.id && running:
			cancel()
			delete(n.parts, p)
			for id, d := range n.docs {
				if d.part == p {
					d.mu.Lock()
					d.removed = true
					d.closeAll()
					d.mu.Unlock()
					delete(n.docs, id)
				}
			}
		}
	}
}

func (n *Node) consume(ctx context.Context, part int, offset uint64) {
	for {
		recs, err := n.d.Bus.Read(ctx, part, offset, 256)
		if err != nil {
			return
		}
		offset += uint64(len(recs))
		for _, c := range recs {
			n.mu.Lock()
			d := n.docs[c.DocID]
			n.mu.Unlock()
			if d != nil {
				n.deliver(d, c)
			}
		}
	}
}

func (n *Node) deliver(d *docState, c domain.Change) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.loaded || d.removed || c.Seq <= d.seq {
		return
	}
	if c.Seq > d.seq+1 {
		n.backfill(d, c.Seq-1)
	}
	if c.Seq == d.seq+1 {
		if err := d.apply(c, n.cfg.Tail); err != nil {
			n.d.Log.Error("apply change", "doc", d.id, "seq", c.Seq, "err", err)
			return
		}
		n.stats.Delivered.Add(1)
	}
}

// backfill 從 operation store 補上 queue 裡缺少的 change（呼叫者持有 d.mu）。
// 需要的 change 已經被 compaction 清掉時，改從 snapshot 重新載入，並把全文送給所有觀看者。
func (n *Node) backfill(d *docState, upTo int64) {
	changes, err := n.d.Store.ChangesSince(n.ctx, d.id, d.seq, upTo)
	if errors.Is(err, store.ErrCompacted) {
		if err := n.load(d); err != nil {
			n.d.Log.Error("reload doc", "doc", d.id, "err", err)
			return
		}
		d.push(d.snapshotEvent())
		return
	}
	if err != nil {
		n.d.Log.Error("backfill", "doc", d.id, "err", err)
		return
	}
	for _, c := range changes {
		if err := d.apply(c, n.cfg.Tail); err != nil {
			n.d.Log.Error("apply change", "doc", d.id, "seq", c.Seq, "err", err)
			return
		}
	}
	n.stats.Backfilled.Add(int64(len(changes)))
}

func (n *Node) load(d *docState) error {
	snap, changes, err := n.d.Store.Load(n.ctx, d.id)
	if err != nil {
		return err
	}
	text := []rune(snap.Text)
	for _, c := range changes {
		if text, err = c.Op.Apply(text); err != nil {
			return err
		}
	}
	d.text, d.seq, d.loaded, d.lastEvent = text, snap.Seq+int64(len(changes)), true, time.Now()
	d.tail = changes[max(0, len(changes)-n.cfg.Tail):]
	return nil
}

func (n *Node) reconcileLoop() {
	t := time.NewTicker(n.cfg.Reconcile)
	defer t.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
		}
		n.mu.Lock()
		for id, d := range n.docs {
			d.mu.Lock()
			if d.loaded && len(d.subs) == 0 {
				d.removed = true
				delete(n.docs, id)
			}
			d.mu.Unlock()
		}
		tracked := make([]*docState, 0, len(n.docs))
		for _, d := range n.docs {
			tracked = append(tracked, d)
		}
		n.mu.Unlock()
		for _, d := range tracked {
			n.reconcile(d)
		}
	}
}

func (n *Node) reconcile(d *docState) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.loaded || d.removed || time.Since(d.lastEvent) < n.cfg.Reconcile {
		return
	}
	d.lastEvent = time.Now()
	head, err := n.d.Store.Head(n.ctx, d.id)
	if err == nil && head > d.seq {
		n.backfill(d, head)
	}
}

// track 回傳文件的狀態，第一次有人觀看時從 operation store 載入。
func (n *Node) track(docID string, part int) (*docState, error) {
	for {
		n.mu.Lock()
		if _, ok := n.parts[part]; !ok {
			n.mu.Unlock()
			return nil, errMoved
		}
		d := n.docs[docID]
		if d == nil {
			d = &docState{id: docID, part: part, subs: make(map[*subscriber]struct{})}
			n.docs[docID] = d
		}
		n.mu.Unlock()

		d.mu.Lock()
		if d.removed {
			d.mu.Unlock()
			continue
		}
		if !d.loaded {
			if err := n.load(d); err != nil {
				d.mu.Unlock()
				return nil, err
			}
		}
		return d, nil // 呼叫者負責 d.mu.Unlock
	}
}

func (n *Node) handleSSE(w http.ResponseWriter, r *http.Request) {
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
	if _, err := n.d.Docs.Role(r.Context(), docID, user); err != nil {
		http.Error(w, "no access", http.StatusForbidden)
		return
	}
	resume := int64(-1)
	if v := r.Header.Get("Last-Event-ID"); v != "" {
		if resume, err = strconv.ParseInt(v, 10, 64); err != nil {
			resume = -1
		}
	}
	d, err := n.track(docID, n.d.Bus.PartitionFor(docID))
	if errors.Is(err, errMoved) {
		http.Error(w, err.Error(), http.StatusMisdirectedRequest)
		return
	}
	if err != nil {
		http.Error(w, "load failed", http.StatusInternalServerError)
		return
	}
	sub, initial, resumed := d.subscribe(resume, n.cfg.SendBuffer)
	d.mu.Unlock()
	if resumed {
		n.stats.Resumed.Add(1)
	} else {
		n.stats.Snapshots.Add(1)
	}
	defer func() {
		d.mu.Lock()
		d.drop(sub)
		d.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	flusher, _ := w.(http.Flusher)
	for _, e := range initial {
		if writeEvent(w, e) != nil {
			return
		}
	}
	if flusher != nil {
		flusher.Flush()
	}
	for {
		select {
		case e, ok := <-sub.ch:
			if !ok {
				return
			}
			if writeEvent(w, e) != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		case <-r.Context().Done():
			return
		case <-n.ctx.Done():
			return
		}
	}
}

func writeEvent(w http.ResponseWriter, e event) error {
	data, err := json.Marshal(e.data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", e.data.Seq, e.name, data)
	return err
}

// Kill 模擬 node 當機：SSE 連線直接斷掉，觀看者用 Last-Event-ID 重連到接手的 node。
func (n *Node) Kill() {
	n.cancel()
	n.srv.Close()
}
