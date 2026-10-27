package registry

import (
	"context"
	"testing"
	"time"
)

func TestExpiredNodesAreRemovedAndMustRegisterAgain(t *testing.T) {
	r := New(100 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	watch := r.Watch(KindEdit)
	<-watch

	r.Register(Node{ID: "edit-0", Kind: KindEdit, Addr: "a:1"})
	r.Register(Node{ID: "edit-1", Kind: KindEdit, Addr: "a:2"})
	r.Register(Node{ID: "view-0", Kind: KindView, Addr: "a:3"})
	m := <-watch
	if ids := m.IDs(); len(ids) != 2 || ids[0] != "edit-0" || ids[1] != "edit-1" {
		t.Fatalf("edit members = %v (the watcher keeps only the latest list)", ids)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		r.Heartbeat("edit-0")
		if len(r.Members(KindEdit).Nodes) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	m = <-watch
	if ids := m.IDs(); len(ids) != 1 || ids[0] != "edit-0" {
		t.Fatalf("after edit-1 stopped heartbeating: %v", ids)
	}
	if r.Heartbeat("edit-1") {
		t.Fatal("an expired node renewed its lease without registering again")
	}
	if addr, ok := m.Addr("edit-0"); !ok || addr != "a:1" {
		t.Fatalf("addr = %q", addr)
	}
}
