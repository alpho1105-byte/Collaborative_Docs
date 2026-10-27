package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"collaborative-docs/internal/client"
	"collaborative-docs/internal/metrics"
	"collaborative-docs/internal/protocol"
)

var failures int

func check(desc string, ok bool) {
	mark := "ok  "
	if !ok {
		mark = "FAIL"
		failures++
	}
	fmt.Printf("  [%s] %s\n", mark, desc)
}

func waitFor(timeout time.Duration, cond func() bool) error {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			return errors.New("timed out")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

// ms 格式化延遲。Windows 上 time.Now 約每 0.5ms 才前進一格，比這更短的間隔量起來是 0。
func ms(d time.Duration) string {
	if d < 500*time.Microsecond {
		return "<0.5ms"
	}
	if d >= time.Second {
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
}

func latencyLine(name string, l *metrics.Latency) string {
	sum := l.Summary()
	return fmt.Sprintf("%-22s p50 %s  p95 %s  p99 %s  max %s  (%d samples)", name, ms(sum.P50), ms(sum.P95), ms(sum.P99), ms(sum.Max), sum.Count)
}

type editorView struct {
	text   string
	rev    int64
	synced bool
	online bool
	cursor int
	remote map[string]protocol.Cursor
}

func view(ag *agent) editorView {
	var v editorView
	ag.ed.Read(func(d *client.Doc) {
		v = editorView{text: d.Text(), rev: d.Rev(), synced: d.Synced(), online: d.Online(), cursor: d.Cursor(), remote: d.Remote()}
	})
	return v
}

// quiesce 等到每個編輯者都在線、沒有待確認的編輯並停在 operation store 的最新版本，觀看者也追上。
func (s *sim) quiesce(ctx context.Context) error {
	err := waitFor(30*time.Second, func() bool {
		for _, d := range s.docs {
			head, err := s.app.Store.Head(ctx, d.id)
			if err != nil {
				return false
			}
			for _, ag := range d.agents {
				if v := view(ag); !v.online || !v.synced || v.rev != head {
					return false
				}
			}
			for _, vw := range d.viewers {
				if _, seq := vw.Text(); seq != head {
					return false
				}
			}
		}
		return true
	})
	if err != nil {
		for _, d := range s.docs {
			head, _ := s.app.Store.Head(ctx, d.id)
			for _, ag := range d.agents {
				if v := view(ag); !v.online || !v.synced || v.rev != head {
					fmt.Printf("  stuck editor %d on %s: online=%v synced=%v rev=%d head=%d\n", ag.id, d.id, v.online, v.synced, v.rev, head)
				}
			}
			for i, vw := range d.viewers {
				if _, seq := vw.Text(); seq != head {
					fmt.Printf("  stuck viewer %d on %s: seq=%d head=%d\n", i, d.id, seq, head)
				}
			}
		}
		return fmt.Errorf("quiesce: %w", err)
	}
	return nil
}

func (s *sim) report(ctx context.Context) {
	var sent, resent, acked, reconnects, discarded int64
	for _, d := range s.docs {
		for _, ag := range d.agents {
			st := ag.ed.Stats()
			sent += st.Sent.Load()
			resent += st.Resent.Load()
			acked += st.Acked.Load()
			reconnects += st.Reconnects.Load()
			discarded += st.Discarded.Load()
		}
	}
	fmt.Printf("edits     %d sent, %d acknowledged, %d resent after reconnecting, %d reconnect attempts, %d pending edits discarded\n",
		sent, acked, resent, reconnects, discarded)
	for _, n := range s.app.EditNodes() {
		st := n.Stats()
		fmt.Printf("%-9s %-7s applied %5d, duplicates ignored %3d, handoffs %3d, fenced writes %2d, loads %3d (replayed %d changes)\n",
			"", n.ID(), st.Applied.Load(), st.Duplicates.Load(), st.Handoffs.Load(), st.Fenced.Load(), st.Loads.Load(), st.Replayed.Load())
	}
	batches, written := s.app.Store.WriteStats()
	stored, _ := s.app.Store.CountChanges(ctx)
	cs := s.app.Compactor.Stats()
	fmt.Printf("store     %d changes in %d commits (%.1f per fsync); compaction: %d snapshots covering %d changes, %d deleted, %d still stored\n",
		written, batches, float64(written)/float64(max(batches, 1)), cs.Snapshots.Load(), cs.Compacted.Load(), cs.Deleted.Load(), stored)
	var delivered, backfilled, resumed, snapshots int64
	for _, n := range s.app.ViewNodes() {
		st := n.Stats()
		delivered += st.Delivered.Load()
		backfilled += st.Backfilled.Load()
		resumed += st.Resumed.Load()
		snapshots += st.Snapshots.Load()
	}
	fmt.Printf("viewers   %d changes from the queue, %d backfilled from the store; streams opened: %d resumed from Last-Event-ID, %d with the full text\n",
		delivered, backfilled, resumed, snapshots)
	fmt.Printf("latency   %s\n", latencyLine("edit -> ack", &s.lat.ack))
	fmt.Printf("          %s\n", latencyLine("edit -> other editors", &s.lat.remote))
	fmt.Printf("          %s\n", latencyLine("edit -> viewers", &s.lat.viewer))
	fmt.Printf("          (only edits typed while connected; edits typed offline wait for the reconnect)\n\n")
}

func (s *sim) verify(ctx context.Context) {
	fmt.Println("checks")

	allSame := true
	for _, d := range s.docs {
		snap, changes, err := s.app.Store.Load(ctx, d.id)
		if err != nil {
			allSame = false
			continue
		}
		stored := snap.Text
		for _, c := range changes {
			if stored, err = c.Op.ApplyString(stored); err != nil {
				allSame = false
			}
		}
		for _, ag := range d.agents {
			if view(ag).text != stored {
				allSame = false
			}
		}
		for _, v := range d.viewers {
			if text, _ := v.Text(); text != stored {
				allSame = false
			}
		}
	}
	check(fmt.Sprintf("all editors, viewers and the operation store (latest snapshot + later changes) hold the same text in all %d documents", len(s.docs)), allSame)

	survived, deleted, intact := 0, 0, true
	for _, d := range s.docs {
		spans, stray := tokenize([]rune(view(d.agents[0]).text))
		seen := make(map[string]int, len(spans))
		for _, sp := range spans {
			seen[sp.text]++
		}
		want := 0
		for tok := range d.inserted {
			if d.deleted[tok] {
				deleted++
				if seen[tok] != 0 {
					intact = false
				}
				continue
			}
			want++
			if seen[tok] != 1 {
				intact = false
			}
		}
		survived += want
		if stray != 0 || len(spans) != want {
			intact = false
		}
	}
	check(fmt.Sprintf("every document splits into whole tokens: all %d surviving inserts appear exactly once and none of the %d deleted tokens remain", survived, deleted), intact)

	var sent, acked, discarded int64
	for _, d := range s.docs {
		for _, ag := range d.agents {
			st := ag.ed.Stats()
			sent, acked, discarded = sent+st.Sent.Load(), acked+st.Acked.Load(), discarded+st.Discarded.Load()
		}
	}
	check(fmt.Sprintf("every edit was acknowledged exactly once (%d sent, %d acked) and no pending edit was discarded", sent, acked), sent == acked && discarded == 0)

	// 最後一次游標回報要等編輯者同步之後才送出，給它一點時間。
	cursorsOK := waitFor(3*time.Second, func() bool { return s.cursorsConsistent() }) == nil
	check("every editor sees every other editor's cursor exactly where that editor has it, always on a token boundary", cursorsOK)

	var fenced, handoffs, loads, duplicates int64
	for _, n := range s.app.EditNodes() {
		st := n.Stats()
		fenced, handoffs, loads, duplicates = fenced+st.Fenced.Load(), handoffs+st.Handoffs.Load(), loads+st.Loads.Load(), duplicates+st.Duplicates.Load()
	}
	zombieFenced := s.app.EditNodes()[2].Stats().Fenced.Load()
	check(fmt.Sprintf("the zombie node lost ownership of %d documents and its writes after that were fenced (%d)", len(s.zombieDocs), zombieFenced), zombieFenced > 0)

	var resent int64
	for _, d := range s.docs {
		for _, ag := range d.agents {
			resent += ag.ed.Stats().Resent.Load()
		}
	}
	check(fmt.Sprintf("documents changed owner %d times (graceful handoffs %d); editors resent %d unacknowledged edits and the owners ignored %d duplicates",
		loads-int64(len(s.docs)), handoffs, resent, duplicates), handoffs > 0 && resent > 0)

	var gaps, viewerReconnects, resumed int64
	for _, d := range s.docs {
		for _, v := range d.viewers {
			gaps += v.Stats().Gaps.Load()
			viewerReconnects += v.Stats().Reconnects.Load()
		}
	}
	for _, n := range s.app.ViewNodes() {
		resumed += n.Stats().Resumed.Load()
	}
	check(fmt.Sprintf("viewers applied every change in seq order with no gaps (%d reconnects, %d resumed from Last-Event-ID)", viewerReconnects, resumed), gaps == 0 && resumed > 0)

	cs := s.app.Compactor.Stats()
	stored, _ := s.app.Store.CountChanges(ctx)
	_, written := s.app.Store.WriteStats()
	check(fmt.Sprintf("compaction wrote %d snapshots and deleted %d covered changes; the store keeps %d of %d changes", cs.Snapshots.Load(), cs.Deleted.Load(), stored, written),
		cs.Snapshots.Load() > 0 && cs.Deleted.Load() > 0 && stored < written)

	check(fmt.Sprintf("the 101st editor on the hot document was rejected (%v)", s.probe), errors.Is(s.probe, client.ErrDocFull))

	ack := s.lat.ack.Summary()
	check(fmt.Sprintf("edit -> ack p99 %s is under the 100ms keystroke budget", ms(ack.P99)), ack.Count > 0 && ack.P99 < 100*time.Millisecond)
}

// cursorsConsistent 比對每個觀察者看到的游標與游標主人自己的位置。
func (s *sim) cursorsConsistent() bool {
	for _, d := range s.docs {
		views := make([]editorView, len(d.agents))
		for i, ag := range d.agents {
			views[i] = view(ag)
		}
		spans, _ := tokenize([]rune(views[0].text))
		for i, obs := range views {
			if len(obs.remote) != len(d.agents)-1 {
				return false
			}
			for j, other := range d.agents {
				if i == j {
					continue
				}
				c, ok := obs.remote[other.ed.ClientID()]
				if !ok || c.Position != views[j].cursor || !onBoundary(spans, c.Position) {
					return false
				}
			}
		}
	}
	return true
}
