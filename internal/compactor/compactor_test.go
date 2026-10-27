package compactor

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"collaborative-docs/internal/domain"
	"collaborative-docs/internal/ot"
	"collaborative-docs/internal/store"
)

func TestCompactionComposesChangesIntoASnapshot(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "docs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	st.CreateDoc(ctx, domain.Doc{ID: "d1", Title: "d1", CreatedBy: "alice", CreatedAt: time.Now()})
	epoch, _ := st.Claim(ctx, "d1", "edit-0")

	text := []rune{}
	var changes []domain.Change
	for i, edit := range []func(n int) ot.Op{
		func(n int) ot.Op { o, _ := ot.NewInsert(n, 0, "Hello"); return o },
		func(n int) ot.Op { o, _ := ot.NewInsert(n, n, " world"); return o },
		func(n int) ot.Op { o, _ := ot.NewDelete(n, 0, 1); return o },
		func(n int) ot.Op { o, _ := ot.NewInsert(n, 0, "J"); return o },
	} {
		op := edit(len(text))
		text, _ = op.Apply(text)
		changes = append(changes, domain.Change{DocID: "d1", Seq: int64(i + 1), ClientID: "c1", ClientSeq: int64(i + 1), UserID: "alice", Op: op,
			At: time.Now().Add(-time.Hour)})
	}
	done := make(chan error, 1)
	st.AppendAsync("d1", epoch, changes, func(err error) { done <- err })
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	c := New(Config{MinChanges: 3, Retain: time.Minute}, st, slog.New(slog.DiscardHandler))
	if err := c.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snap, rest, err := st.Load(ctx, "d1")
	if err != nil || snap.Seq != 4 || snap.Text != "Jello world" || len(rest) != 0 || snap.Clients["c1"].ClientSeq != 4 {
		t.Fatalf("snapshot %+v, %d changes left, %v", snap, len(rest), err)
	}
	if c.Stats().Deleted.Load() != 4 {
		t.Fatalf("deleted %d old changes, want 4", c.Stats().Deleted.Load())
	}
}
