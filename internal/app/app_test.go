package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"collaborative-docs/internal/client"
	"collaborative-docs/internal/domain"
)

type cluster struct {
	t   *testing.T
	ctx context.Context
	app *App
	gw  string
}

func newCluster(t *testing.T, editNodes int, tune func(*Options)) *cluster {
	t.Helper()
	opts := DefaultOptions(filepath.Join(t.TempDir(), "docs.db"), []byte("secret"))
	opts.Dev = true
	opts.Logger = slog.New(slog.DiscardHandler)
	opts.LeaseTTL = 300 * time.Millisecond
	opts.Editor.Heartbeat = 50 * time.Millisecond
	opts.Viewer.Heartbeat = 50 * time.Millisecond
	if tune != nil {
		tune(&opts)
	}
	a, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	a.Start(ctx)
	c := &cluster{t: t, ctx: ctx, app: a}
	for range editNodes {
		c.addEditNode()
	}
	c.app.AddViewNode(c.listen(), "")
	gw := httptest.NewServer(a.Gateway.Handler())
	c.gw = gw.URL
	t.Cleanup(func() {
		gw.Close()
		cancel()
		time.Sleep(50 * time.Millisecond)
		a.Close()
	})
	return c
}

func (c *cluster) listen() net.Listener {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatal(err)
	}
	return ln
}

func (c *cluster) addEditNode() { c.app.AddEditNode(c.listen(), "") }

func (c *cluster) login(user string) *client.API {
	api, err := client.Login(c.ctx, c.gw, user)
	if err != nil {
		c.t.Fatal(err)
	}
	return api
}

func (c *cluster) editor(api *client.API, docID string) *client.Editor {
	e := client.NewEditor(api, client.EditorConfig{DocID: docID})
	if err := e.Start(c.ctx); err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(e.Close)
	return e
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func state(e *client.Editor) (text string, rev int64, synced bool) {
	e.Read(func(d *client.Doc) { text, rev, synced = d.Text(), d.Rev(), d.Synced() })
	return
}

func typeRandomly(t *testing.T, e *client.Editor, r *rand.Rand, n int, mark string) {
	for i := range n {
		err := e.Update(func(d *client.Doc) error {
			if d.Len() > 0 && r.IntN(4) == 0 {
				return d.Delete(r.IntN(d.Len()), 1)
			}
			return d.Insert(r.IntN(d.Len()+1), fmt.Sprintf("%s%d", mark, i))
		})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(r.IntN(3)) * time.Millisecond)
	}
}

// converged 等到所有編輯者都沒有待確認的編輯、停在同一個版本，而且觀看者也追上。
func converged(t *testing.T, editors []*client.Editor, viewers []*client.Viewer) string {
	t.Helper()
	var want string
	waitFor(t, "convergence", func() bool {
		text0, rev0, ok := state(editors[0])
		if !ok {
			return false
		}
		for _, e := range editors[1:] {
			if text, rev, synced := state(e); !synced || rev != rev0 || text != text0 {
				return false
			}
		}
		for _, v := range viewers {
			if text, seq := v.Text(); seq != rev0 || text != text0 {
				return false
			}
		}
		want = text0
		return true
	})
	return want
}

func TestConcurrentEditorsAndAViewerConverge(t *testing.T) {
	c := newCluster(t, 2, nil)
	alice, bob, carol := c.login("alice"), c.login("bob"), c.login("carol")
	docID, err := alice.CreateDoc(c.ctx, "notes")
	if err != nil {
		t.Fatal(err)
	}
	alice.Share(c.ctx, docID, "bob", domain.RoleEditor)
	alice.Share(c.ctx, docID, "carol", domain.RoleViewer)

	if _, err := carol.EditURL(c.ctx, docID); err == nil {
		t.Fatal("a viewer got an edit URL")
	}
	if err := client.NewEditor(carol, client.EditorConfig{DocID: docID}).Start(c.ctx); !errors.Is(err, client.ErrNoAccess) {
		t.Fatalf("viewer joining as editor: %v", err)
	}

	viewer := client.NewViewer(carol, docID, nil)
	viewer.Start(c.ctx)
	t.Cleanup(viewer.Close)
	editors := []*client.Editor{c.editor(alice, docID), c.editor(bob, docID)}
	done := make(chan struct{})
	for i, e := range editors {
		go func() {
			typeRandomly(t, e, rand.New(rand.NewPCG(uint64(i), 1)), 200, string(rune('A'+i)))
			done <- struct{}{}
		}()
	}
	<-done
	<-done
	text := converged(t, editors, []*client.Viewer{viewer})
	if text == "" {
		t.Fatal("document is empty")
	}
}

// 加入新的 node 後，consistent hashing 把部分文件交給它；舊 owner 交接時不能弄丟任何編輯。
func TestDocumentsMoveToANewNodeWithoutLosingEdits(t *testing.T) {
	c := newCluster(t, 1, nil)
	alice := c.login("alice")
	type doc struct {
		editors []*client.Editor
		viewer  *client.Viewer
	}
	var docs []doc
	for i := range 8 {
		docID, _ := alice.CreateDoc(c.ctx, fmt.Sprintf("doc %d", i))
		v := client.NewViewer(alice, docID, nil)
		v.Start(c.ctx)
		t.Cleanup(v.Close)
		docs = append(docs, doc{editors: []*client.Editor{c.editor(alice, docID), c.editor(alice, docID)}, viewer: v})
	}
	done := make(chan struct{})
	for i, d := range docs {
		for j, e := range d.editors {
			go func() {
				typeRandomly(t, e, rand.New(rand.NewPCG(uint64(i), uint64(j))), 150, string(rune('a'+j)))
				done <- struct{}{}
			}()
		}
	}
	time.Sleep(50 * time.Millisecond)
	c.addEditNode()
	for range 2 * len(docs) {
		<-done
	}
	for _, d := range docs {
		converged(t, d.editors, []*client.Viewer{d.viewer})
	}
	if n := c.app.EditNodes()[0].Stats().Handoffs.Load(); n == 0 {
		t.Fatal("no document was handed off to the new node")
	}
}

func TestEditorLimitRejectsTheNextEditor(t *testing.T) {
	c := newCluster(t, 1, func(o *Options) { o.Editor.MaxEditors = 2 })
	alice := c.login("alice")
	docID, _ := alice.CreateDoc(c.ctx, "busy")
	c.editor(alice, docID)
	c.editor(alice, docID)
	if err := client.NewEditor(alice, client.EditorConfig{DocID: docID}).Start(c.ctx); !errors.Is(err, client.ErrDocFull) {
		t.Fatalf("third editor: %v", err)
	}
}

func TestGatewayDocsAPI(t *testing.T) {
	c := newCluster(t, 1, nil)
	alice, bob := c.login("alice"), c.login("bob")
	if _, err := alice.CreateDoc(c.ctx, "   "); err == nil {
		t.Fatal("blank title accepted")
	}
	docID, _ := alice.CreateDoc(c.ctx, "plan")
	var he *client.HTTPError
	if err := bob.Share(c.ctx, docID, "carol", domain.RoleEditor); !errors.As(err, &he) || he.Status != http.StatusNotFound {
		t.Fatalf("non-member sharing: %v", err)
	}
	alice.Share(c.ctx, docID, "bob", domain.RoleEditor)
	if err := bob.Share(c.ctx, docID, "carol", domain.RoleEditor); !errors.As(err, &he) || he.Status != http.StatusForbidden {
		t.Fatalf("editor sharing: %v", err)
	}
	if err := alice.Share(c.ctx, docID, "carol", domain.RoleOwner); !errors.As(err, &he) || he.Status != http.StatusUnprocessableEntity {
		t.Fatalf("sharing the owner role: %v", err)
	}
	if _, err := client.NewAPI(c.gw, "bad-token").ViewURL(c.ctx, docID); !errors.As(err, &he) || he.Status != http.StatusUnauthorized {
		t.Fatalf("bad token: %v", err)
	}
}
