package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"collaborative-docs/internal/domain"
	"collaborative-docs/internal/ot"
)

func openStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "docs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func createDoc(t *testing.T, s *Store, id string) {
	t.Helper()
	if err := s.CreateDoc(context.Background(), domain.Doc{ID: id, Title: id, CreatedBy: "alice", CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
}

func insertChange(seq int64, docLen int, text string) domain.Change {
	op, _ := ot.NewInsert(docLen, docLen, text)
	return domain.Change{Seq: seq, ClientID: "c1", ClientSeq: seq, UserID: "alice", Op: op, At: time.Now()}
}

func appendSync(s *Store, docID string, epoch int64, changes ...domain.Change) error {
	done := make(chan error, 1)
	for i := range changes {
		changes[i].DocID = docID
	}
	s.AppendAsync(docID, epoch, changes, func(err error) { done <- err })
	return <-done
}

// 新 owner claim 之後，舊 owner 的寫入必須失敗；同一批裡其他文件的寫入不受影響。
func TestClaimFencesThePreviousOwner(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	createDoc(t, s, "d1")
	createDoc(t, s, "d2")
	old, _ := s.Claim(ctx, "d1", "edit-0")
	other, _ := s.Claim(ctx, "d2", "edit-0")
	if err := appendSync(s, "d1", old, insertChange(1, 0, "a")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx, "d1", "edit-1"); err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 2)
	s.AppendAsync("d1", old, []domain.Change{{DocID: "d1", Seq: 2, ClientID: "c1", ClientSeq: 2, Op: insertChange(2, 1, "b").Op, At: time.Now()}},
		func(err error) { results <- err })
	s.AppendAsync("d2", other, []domain.Change{{DocID: "d2", Seq: 1, ClientID: "c2", ClientSeq: 1, Op: insertChange(1, 0, "x").Op, At: time.Now()}},
		func(err error) { results <- err })
	if err := <-results; !errors.Is(err, ErrFenced) {
		t.Fatalf("stale owner: err = %v", err)
	}
	if err := <-results; err != nil {
		t.Fatalf("other document in the same batch: %v", err)
	}
	if head, _ := s.Head(ctx, "d1"); head != 1 {
		t.Fatalf("d1 head = %d, the fenced write leaked", head)
	}
	if head, _ := s.Head(ctx, "d2"); head != 1 {
		t.Fatalf("d2 head = %d", head)
	}
}

func TestLoadAndCatchUpAfterCompaction(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	createDoc(t, s, "d1")
	epoch, _ := s.Claim(ctx, "d1", "edit-0")
	for i := range 6 {
		if err := appendSync(s, "d1", epoch, insertChange(int64(i+1), i, "x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SaveSnapshot(ctx, domain.Snapshot{DocID: "d1", Seq: 4, Text: "xxxx", Clients: map[string]domain.ClientMark{"c1": {ClientSeq: 4, Seq: 4}}}); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.GC(ctx, "d1", 4, time.Now().Add(time.Second)); n != 4 {
		t.Fatalf("gc deleted %d changes, want 4", n)
	}

	snap, changes, err := s.Load(ctx, "d1")
	if err != nil || snap.Seq != 4 || len(changes) != 2 || changes[0].Seq != 5 || snap.Clients["c1"].ClientSeq != 4 {
		t.Fatalf("load = %+v %d changes, %v", snap, len(changes), err)
	}
	if got, err := s.ChangesSince(ctx, "d1", 4, 6); err != nil || len(got) != 2 {
		t.Fatalf("since 4 = %d, %v", len(got), err)
	}
	if _, err := s.ChangesSince(ctx, "d1", 2, 6); !errors.Is(err, ErrCompacted) {
		t.Fatalf("since 2 after gc: err = %v", err)
	}
	if ids, _ := s.CompactionCandidates(ctx, 2); len(ids) != 1 {
		t.Fatalf("candidates = %v", ids)
	}
}

func TestGCKeepsRecentChanges(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	createDoc(t, s, "d1")
	epoch, _ := s.Claim(ctx, "d1", "edit-0")
	appendSync(s, "d1", epoch, insertChange(1, 0, "x"))
	s.SaveSnapshot(ctx, domain.Snapshot{DocID: "d1", Seq: 1, Text: "x"})
	if n, _ := s.GC(ctx, "d1", 1, time.Now().Add(-time.Minute)); n != 0 {
		t.Fatalf("gc deleted %d changes that are still inside the retention window", n)
	}
}
