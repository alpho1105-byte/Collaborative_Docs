package viewer

import (
	"bufio"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"collaborative-docs/internal/auth"
	"collaborative-docs/internal/docs"
	"collaborative-docs/internal/domain"
	"collaborative-docs/internal/eventbus"
	"collaborative-docs/internal/ot"
	"collaborative-docs/internal/protocol"
	"collaborative-docs/internal/registry"
	"collaborative-docs/internal/store"
)

type rig struct {
	t      *testing.T
	ctx    context.Context
	store  *store.Store
	bus    *eventbus.Topic[domain.Change]
	signer *auth.Signer
	addr   string
	docID  string
	epoch  int64
	text   []rune
	seq    int64
}

func newRig(t *testing.T) *rig {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "docs.db"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &rig{t: t, ctx: ctx, store: st, bus: eventbus.New[domain.Change](1), signer: auth.NewSigner([]byte("secret"))}
	ds := docs.New(st)
	cfg := DefaultConfig()
	cfg.Reconcile = 100 * time.Millisecond
	cfg.Tail = 4
	n := New("view-0", cfg, Deps{Store: st, Docs: ds, Registry: registry.New(time.Minute), Signer: r.signer, Bus: r.bus,
		Log: slog.New(slog.DiscardHandler)})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	n.Start(ctx, ln, "")
	r.addr = ln.Addr().String()
	t.Cleanup(func() {
		cancel()
		n.Kill()
		time.Sleep(20 * time.Millisecond)
		st.Close()
	})
	for deadline := time.Now().Add(5 * time.Second); ; {
		n.mu.Lock()
		owned := len(n.parts) == 1
		n.mu.Unlock()
		if owned {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("viewer node never took the partition")
		}
		time.Sleep(5 * time.Millisecond)
	}
	d, _ := ds.Create(ctx, "alice", "test")
	r.docID = d.ID
	r.epoch, _ = st.Claim(ctx, d.ID, "edit-0")
	return r
}

// write 模擬 edit owner：寫入 operation store，publish 為 true 時才發佈到 queue。
func (r *rig) write(text string, publish bool) {
	r.t.Helper()
	op, _ := ot.NewInsert(len(r.text), len(r.text), text)
	r.text, _ = op.Apply(r.text)
	r.seq++
	c := domain.Change{DocID: r.docID, Seq: r.seq, ClientID: "c1", ClientSeq: r.seq, UserID: "alice", Op: op, At: time.Now()}
	done := make(chan error, 1)
	r.store.AppendAsync(r.docID, r.epoch, []domain.Change{c}, func(err error) { done <- err })
	if err := <-done; err != nil {
		r.t.Fatal(err)
	}
	if publish {
		r.bus.Append(0, c)
	}
}

type stream struct {
	events chan sseEvent
	cancel context.CancelFunc
}

type sseEvent struct {
	name string
	data protocol.ViewerEvent
}

func (r *rig) open(resume int64) *stream {
	r.t.Helper()
	ctx, cancel := context.WithCancel(r.ctx)
	tok, _ := r.signer.Issue("alice", time.Hour)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+r.addr+"/sse/docs/"+r.docID, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	if resume >= 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(resume, 10))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		r.t.Fatalf("open stream: %v %v", err, resp)
	}
	s := &stream{events: make(chan sseEvent, 64), cancel: cancel}
	go func() {
		defer resp.Body.Close()
		sc := bufio.NewScanner(resp.Body)
		var name string
		for sc.Scan() {
			line := sc.Text()
			if v, ok := strings.CutPrefix(line, "event: "); ok {
				name = v
			}
			if v, ok := strings.CutPrefix(line, "data: "); ok {
				var ev protocol.ViewerEvent
				json.Unmarshal([]byte(v), &ev)
				s.events <- sseEvent{name: name, data: ev}
			}
		}
	}()
	r.t.Cleanup(cancel)
	return s
}

func (r *rig) next(s *stream) sseEvent {
	r.t.Helper()
	select {
	case e := <-s.events:
		return e
	case <-time.After(5 * time.Second):
		r.t.Fatal("no event")
		return sseEvent{}
	}
}

func (r *rig) expectOps(s *stream, seqs ...int64) {
	r.t.Helper()
	for _, want := range seqs {
		if e := r.next(s); e.name != "op" || e.data.Seq != want {
			r.t.Fatalf("got %s %d, want op %d", e.name, e.data.Seq, want)
		}
	}
}

func TestViewersReceiveEveryChangeInOrderEvenWhenTheQueueMissesSome(t *testing.T) {
	r := newRig(t)
	r.write("a", false)
	s := r.open(-1)
	if e := r.next(s); e.name != "snapshot" || e.data.Seq != 1 || *e.data.Text != "a" {
		t.Fatalf("first event = %+v", e)
	}
	r.write("b", true)
	r.write("c", false) // owner 寫入後、發佈前當機
	r.write("d", true)
	r.expectOps(s, 2, 3, 4)

	// 最後一個 change 沒有發佈、之後也沒有新的 change：只能靠定期向 store 確認補上。
	r.write("e", false)
	r.expectOps(s, 5)
}

func TestReconnectingViewerResumesFromLastEventID(t *testing.T) {
	r := newRig(t)
	s := r.open(-1)
	r.next(s)
	for _, t := range []string{"a", "b", "c"} {
		r.write(t, true)
	}
	r.expectOps(s, 1, 2, 3)
	s.cancel()

	r.write("d", true)
	resumed := r.open(2)
	r.expectOps(resumed, 3, 4)

	// 落後超過 tail 能補的範圍就改送全文。
	for _, t := range []string{"e", "f", "g", "h", "i", "j"} {
		r.write(t, true)
	}
	behind := r.open(1)
	if e := r.next(behind); e.name != "snapshot" || e.data.Seq != 10 || *e.data.Text != "abcdefghij" {
		t.Fatalf("far behind: %+v", e)
	}
}
