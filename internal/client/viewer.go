package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"collaborative-docs/internal/ot"
	"collaborative-docs/internal/protocol"
)

var errGap = errors.New("missing change in the viewer stream")

type ViewerStats struct {
	Gaps       atomic.Int64
	Reconnects atomic.Int64
}

// Viewer 是唯讀的觀看者：透過 SSE 依 seq 套用 change，不做 OT。
// 斷線後帶著 Last-Event-ID 重連，server 只補缺少的 change。
type Viewer struct {
	api      *API
	docID    string
	onChange func(ot.Op)
	stats    ViewerStats
	http     *http.Client

	mu   sync.Mutex
	text []rune
	seq  int64
	drop context.CancelFunc

	cancel context.CancelFunc
	done   chan struct{}
}

func NewViewer(api *API, docID string, onChange func(ot.Op)) *Viewer {
	return &Viewer{api: api, docID: docID, onChange: onChange, seq: protocol.NoRev, http: &http.Client{}}
}

func (v *Viewer) Stats() *ViewerStats { return &v.stats }

// Text 回傳目前的全文與 seq。
func (v *Viewer) Text() (string, int64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return string(v.text), v.seq
}

func (v *Viewer) Start(ctx context.Context) {
	ctx, v.cancel = context.WithCancel(ctx)
	v.done = make(chan struct{})
	go func() {
		defer close(v.done)
		backoff := 20 * time.Millisecond
		for ctx.Err() == nil {
			if v.stream(ctx) {
				backoff = 20 * time.Millisecond
			} else {
				backoff = min(2*backoff, time.Second)
			}
			v.stats.Reconnects.Add(1)
			select {
			case <-ctx.Done():
			case <-time.After(backoff/2 + rand.N(backoff/2)):
			}
		}
	}()
}

// stream 讀一條 SSE 連線直到中斷，回傳是否成功收到過資料。
func (v *Viewer) stream(ctx context.Context) bool {
	sseURL, err := v.api.ViewURL(ctx, v.docID)
	if err != nil {
		return false
	}
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	v.mu.Lock()
	v.drop = cancel
	seq := v.seq
	v.mu.Unlock()

	req, err := http.NewRequestWithContext(connCtx, http.MethodGet, sseURL, nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+v.api.Token)
	if seq >= 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(seq, 10))
	}
	resp, err := v.http.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 64<<10), 64<<20)
	var name, data string
	received := false
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if err := v.dispatch(name, data); err != nil {
				return received
			}
			received = true
			name, data = "", ""
		case strings.HasPrefix(line, "event: "):
			name = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		}
	}
	return received
}

func (v *Viewer) dispatch(name, data string) error {
	var ev protocol.ViewerEvent
	if err := json.Unmarshal([]byte(data), &ev); err != nil {
		return err
	}
	switch {
	case name == "snapshot" && ev.Text != nil:
		v.mu.Lock()
		v.text, v.seq = []rune(*ev.Text), ev.Seq
		v.mu.Unlock()
	case name == "op" && ev.Op != nil:
		v.mu.Lock()
		if ev.Seq <= v.seq {
			v.mu.Unlock()
			return nil
		}
		if ev.Seq != v.seq+1 {
			v.mu.Unlock()
			v.stats.Gaps.Add(1)
			return fmt.Errorf("%w: got %d at %d", errGap, ev.Seq, v.seq)
		}
		text, err := ev.Op.Apply(v.text)
		if err != nil {
			v.mu.Unlock()
			return err
		}
		v.text, v.seq = text, ev.Seq
		v.mu.Unlock()
		if v.onChange != nil {
			v.onChange(*ev.Op)
		}
	}
	return nil
}

// DropConnection 模擬網路中斷，viewer 會帶著 Last-Event-ID 重連。
func (v *Viewer) DropConnection() {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.drop != nil {
		v.drop()
	}
}

func (v *Viewer) Close() {
	v.cancel()
	<-v.done
}
