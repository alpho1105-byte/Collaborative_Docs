package editor

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"collaborative-docs/internal/auth"
	"collaborative-docs/internal/docs"
	"collaborative-docs/internal/domain"
	"collaborative-docs/internal/ot"
	"collaborative-docs/internal/protocol"
	"collaborative-docs/internal/registry"
	"collaborative-docs/internal/store"
)

type nopPublisher struct{}

func (nopPublisher) Publish(domain.Change) {}

type rig struct {
	t      *testing.T
	ctx    context.Context
	store  *store.Store
	docs   *docs.Service
	signer *auth.Signer
	node   *Node
	docID  string
}

func newRig(t *testing.T, cfg Config) *rig {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "docs.db"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &rig{t: t, ctx: ctx, store: st, docs: docs.New(st), signer: auth.NewSigner([]byte("secret"))}
	r.node = New("edit-0", cfg, Deps{Store: st, Docs: r.docs, Registry: registry.New(time.Minute), Signer: r.signer,
		Publisher: nopPublisher{}, Log: slog.New(slog.DiscardHandler)})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r.node.Start(ctx, ln, "")
	t.Cleanup(func() {
		cancel()
		r.node.Kill()
		time.Sleep(20 * time.Millisecond)
		st.Close()
	})
	for deadline := time.Now().Add(5 * time.Second); !r.node.owns("anything"); {
		if time.Now().After(deadline) {
			t.Fatal("node never saw itself in the registry")
		}
		time.Sleep(5 * time.Millisecond)
	}
	d, err := r.docs.Create(ctx, "alice", "test")
	if err != nil {
		t.Fatal(err)
	}
	r.docID = d.ID
	return r
}

func (r *rig) dial(user string) (*websocket.Conn, *http.Response, error) {
	tok, _ := r.signer.Issue(user, time.Hour)
	return websocket.Dial(r.ctx, "ws://"+r.node.addr+"/ws/docs/"+r.docID+"?token="+tok, nil)
}

func (r *rig) join(clientID string, rev int64) (*websocket.Conn, protocol.ServerFrame) {
	r.t.Helper()
	ws, _, err := r.dial("alice")
	if err != nil {
		r.t.Fatal(err)
	}
	r.t.Cleanup(func() { ws.CloseNow() })
	r.send(ws, protocol.ClientFrame{Type: protocol.TypeJoin, ClientID: clientID, Rev: rev})
	return ws, r.read(ws)
}

func (r *rig) send(ws *websocket.Conn, f protocol.ClientFrame) {
	r.t.Helper()
	if err := wsjson.Write(r.ctx, ws, f); err != nil {
		r.t.Fatal(err)
	}
}

func (r *rig) read(ws *websocket.Conn) protocol.ServerFrame {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()
	var f protocol.ServerFrame
	if err := wsjson.Read(ctx, ws, &f); err != nil {
		r.t.Fatal(err)
	}
	return f
}

// readUntil 略過游標等其他訊息，回傳第一個指定類型的 frame。
func (r *rig) readUntil(ws *websocket.Conn, typ string) protocol.ServerFrame {
	r.t.Helper()
	for {
		if f := r.read(ws); f.Type == typ {
			return f
		}
	}
}

func edit(cseq, rev int64, op ot.Op) protocol.ClientFrame {
	return protocol.ClientFrame{Type: protocol.TypeEdit, CSeq: cseq, Rev: rev, Op: &op}
}

func insert(docLen, pos int, text string) ot.Op {
	op, _ := ot.NewInsert(docLen, pos, text)
	return op
}

func (r *rig) storedText() string {
	r.t.Helper()
	snap, changes, err := r.store.Load(r.ctx, r.docID)
	if err != nil {
		r.t.Fatal(err)
	}
	text := snap.Text
	for _, c := range changes {
		text, _ = c.Op.ApplyString(text)
	}
	return text
}

// 講義的例子走一次真正的 server：A 與 B 都基於 "This is a doc" 送出編輯，A 先到。
// B 的編輯被轉換成 INSERT(18, "!")，A 收到的就是轉換後的版本。
func TestOwnerTransformsConcurrentEditsInArrivalOrder(t *testing.T) {
	r := newRig(t, DefaultConfig())
	a, _ := r.join("A", protocol.NoRev)
	b, _ := r.join("B", protocol.NoRev)
	r.send(a, edit(1, 0, insert(0, 0, "This is a doc")))
	if f := r.readUntil(a, protocol.TypeAck); f.Seq != 1 {
		t.Fatalf("ack = %+v", f)
	}
	r.readUntil(b, protocol.TypeOp)

	r.send(a, edit(2, 1, insert(13, 13, "ument")))
	r.readUntil(a, protocol.TypeAck)
	r.send(b, edit(1, 1, insert(13, 13, "!")))
	if f := r.readUntil(b, protocol.TypeOp); f.Seq != 2 || f.ClientID != "A" {
		t.Fatalf("B should first receive A's edit, got %+v", f)
	}
	if f := r.readUntil(b, protocol.TypeAck); f.Seq != 3 {
		t.Fatalf("B's ack = %+v", f)
	}
	f := r.readUntil(a, protocol.TypeOp)
	if got, _ := json.Marshal(f.Op); string(got) != `[18,"!"]` {
		t.Fatalf("A received %s, want INSERT(18, \"!\")", got)
	}
	if text := r.storedText(); text != "This is a document!" {
		t.Fatalf("stored %q", text)
	}
}

// client 收不到 ack 時會重送同一個 cseq；owner 只回 ack，不能再套用一次。
func TestResentEditIsAcknowledgedButNotAppliedTwice(t *testing.T) {
	r := newRig(t, DefaultConfig())
	a, _ := r.join("A", protocol.NoRev)
	r.send(a, edit(1, 0, insert(0, 0, "hello")))
	r.readUntil(a, protocol.TypeAck)
	r.send(a, edit(1, 0, insert(0, 0, "hello")))
	if f := r.readUntil(a, protocol.TypeAck); f.CSeq != 1 || f.Seq != 1 {
		t.Fatalf("duplicate ack = %+v", f)
	}
	if text := r.storedText(); text != "hello" || r.node.Stats().Duplicates.Load() != 1 {
		t.Fatalf("stored %q, duplicates %d", text, r.node.Stats().Duplicates.Load())
	}

	// 重連時帶舊的 rev：自己的編輯出現在補齊的 change 裡，client 據此知道它已經被套用。
	_, welcome := r.join("A", 0)
	if len(welcome.Changes) != 1 || welcome.Changes[0].ClientID != "A" || welcome.AckedCSeq != 1 {
		t.Fatalf("welcome = %+v", welcome)
	}
}

func TestRejoiningClientCatchesUpFromTheStoreWhenTheWindowIsTooShort(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Window = 2
	r := newRig(t, cfg)
	a, _ := r.join("A", protocol.NoRev)
	for i := range 8 {
		r.send(a, edit(int64(i+1), int64(i), insert(i, i, "x")))
		r.readUntil(a, protocol.TypeAck)
	}
	_, welcome := r.join("B", 1)
	if welcome.Text != nil || len(welcome.Changes) != 7 || welcome.Changes[0].Seq != 2 || welcome.Seq != 8 {
		t.Fatalf("welcome from rev 1: %d changes, text %v, seq %d", len(welcome.Changes), welcome.Text != nil, welcome.Seq)
	}
	_, fresh := r.join("C", protocol.NoRev)
	if fresh.Text == nil || *fresh.Text != "xxxxxxxx" {
		t.Fatalf("fresh client welcome = %+v", fresh)
	}
	// 基於已經不在 window 裡的版本送來的編輯無法轉換，client 必須重新同步。
	r.send(a, edit(9, 1, insert(1, 0, "y")))
	if f := r.readUntil(a, protocol.TypeError); f.Code != protocol.CodeStale {
		t.Fatalf("edit based on an old rev: %+v", f)
	}
}

func TestOwnerThatLostTheDocumentSendsEditorsAway(t *testing.T) {
	r := newRig(t, DefaultConfig())
	a, _ := r.join("A", protocol.NoRev)
	r.send(a, edit(1, 0, insert(0, 0, "mine")))
	r.readUntil(a, protocol.TypeAck)
	if _, err := r.store.Claim(r.ctx, r.docID, "edit-9"); err != nil {
		t.Fatal(err)
	}
	r.send(a, edit(2, 1, insert(4, 4, " too")))
	if f := r.readUntil(a, protocol.TypeMoved); f.Type != protocol.TypeMoved {
		t.Fatal(f)
	}
	if text := r.storedText(); text != "mine" || r.node.Stats().Fenced.Load() != 1 {
		t.Fatalf("stored %q, fenced %d", text, r.node.Stats().Fenced.Load())
	}
}

func TestOnlyEditorsCanOpenTheEditSocket(t *testing.T) {
	r := newRig(t, DefaultConfig())
	r.docs.Share(r.ctx, "alice", r.docID, "carol", domain.RoleViewer)
	for _, user := range []string{"carol", "mallory"} {
		if _, resp, err := r.dial(user); err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
			t.Fatalf("%s: err=%v", user, err)
		}
	}
	if _, resp, err := websocket.Dial(r.ctx, "ws://"+r.node.addr+"/ws/docs/"+r.docID, nil); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("no token: %v %v", err, resp)
	}
}
