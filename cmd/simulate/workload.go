package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"collaborative-docs/internal/client"
	"collaborative-docs/internal/metrics"
	"collaborative-docs/internal/ot"
)

// 模擬插入的最小單位是 token：«編輯者.序號»，全域唯一。插入只落在 token 之間，刪除一次刪掉整個 token，
// 所以最後的文件必須能完整切成 token。轉換算錯位置、重複套用或刪錯範圍，都會讓某個 token 被切斷、
// 消失或出現兩次。
type span struct {
	start, end int
	text       string
}

func tokenize(text []rune) (spans []span, stray int) {
	for i := 0; i < len(text); {
		if text[i] != '«' {
			stray++
			i++
			continue
		}
		j := i + 1
		for j < len(text) && text[j] != '»' && text[j] != '«' {
			j++
		}
		if j == len(text) || text[j] != '»' {
			stray += j - i
			i = j
			continue
		}
		spans = append(spans, span{start: i, end: j + 1, text: string(text[i : j+1])})
		i = j + 1
	}
	return spans, stray
}

func boundaries(spans []span) []int {
	out := []int{0}
	for _, s := range spans {
		out = append(out, s.end)
	}
	return out
}

func onBoundary(spans []span, pos int) bool {
	for _, b := range boundaries(spans) {
		if b == pos {
			return true
		}
	}
	return false
}

type docRun struct {
	id      string
	hot     bool
	agents  []*agent
	viewers []*client.Viewer

	mu       sync.Mutex
	inserted map[string]bool
	deleted  map[string]bool
}

func (d *docRun) record(token string, insert bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if insert {
		d.inserted[token] = true
	} else {
		d.deleted[token] = true
	}
}

// latencies 記錄每個 token 打字的時間，收到含有該 token 的 change 時算出延遲。
type latencies struct {
	mu     sync.Mutex
	typed  map[string]time.Time
	ack    metrics.Latency
	remote metrics.Latency
	viewer metrics.Latency
}

func (l *latencies) typedNow(token string) {
	l.mu.Lock()
	l.typed[token] = time.Now()
	l.mu.Unlock()
}

func (l *latencies) observe(op ot.Op, dst *metrics.Latency) {
	spans, _ := tokenize([]rune(op.InsertedText()))
	now := time.Now()
	for _, s := range spans {
		l.mu.Lock()
		at, ok := l.typed[s.text]
		l.mu.Unlock()
		if ok {
			dst.Observe(now.Sub(at))
		}
	}
}

type counters struct {
	inserts, deletes, moves, blips atomic.Int64
}

type agent struct {
	id   int
	doc  *docRun
	ed   *client.Editor
	rng  *rand.Rand
	next int
}

// act 做一個動作：在隨機的 token 邊界插入新 token（游標移到它後面）、刪掉一個 token，或移動游標。
func (a *agent) act(lat *latencies, c *counters) error {
	return a.ed.Update(func(d *client.Doc) error {
		spans, _ := tokenize([]rune(d.Text()))
		bounds := boundaries(spans)
		switch r := a.rng.IntN(100); {
		case r < 25 && len(spans) > 0:
			s := spans[a.rng.IntN(len(spans))]
			if err := d.Delete(s.start, s.end-s.start); err != nil {
				return err
			}
			a.doc.record(s.text, false)
			c.deletes.Add(1)
		case r < 33:
			d.SetCursor(bounds[a.rng.IntN(len(bounds))])
			c.moves.Add(1)
		default:
			pos := bounds[a.rng.IntN(len(bounds))]
			a.next++
			token := fmt.Sprintf("«%d.%d»", a.id, a.next)
			if err := d.Insert(pos, token); err != nil {
				return err
			}
			d.SetCursor(pos + utf8.RuneCountInString(token))
			a.doc.record(token, true)
			// 離線時打的字要等重連才送得出去，延遲反映的是斷線時間而不是系統，不列入統計。
			if d.Online() {
				lat.typedNow(token)
			}
			c.inserts.Add(1)
		}
		return nil
	})
}

func (a *agent) run(stop <-chan struct{}, gap func(*rand.Rand) time.Duration, lat *latencies, c *counters) error {
	for {
		select {
		case <-stop:
			return nil
		case <-time.After(gap(a.rng)):
		}
		if err := a.act(lat, c); err != nil {
			return fmt.Errorf("agent %d: %w", a.id, err)
		}
	}
}
