// Package compactor 是離線的 snapshot / compaction worker：把上一個 snapshot 之後累積的 change
// compose 成一個編輯、套用成新的 snapshot，再把已經被 snapshot 涵蓋、而且夠舊的 change 清掉。
// 新加入的 client 只需要 snapshot@S 加上 S 之後的 change，不必重播整份歷史。
package compactor

import (
	"context"
	"log/slog"
	"maps"
	"sync/atomic"
	"time"

	"collaborative-docs/internal/domain"
	"collaborative-docs/internal/ot"
	"collaborative-docs/internal/store"
)

type Config struct {
	Every time.Duration
	// MinChanges：上一個 snapshot 之後累積多少個 change 才值得做一次 compaction。
	MinChanges int
	// Retain：被 snapshot 涵蓋的 change 至少保留這麼久，讓短暫斷線的 client 還能只補差異。
	Retain time.Duration
}

func DefaultConfig() Config {
	return Config{Every: time.Minute, MinChanges: 2000, Retain: 15 * time.Minute}
}

type Stats struct {
	Snapshots atomic.Int64
	Compacted atomic.Int64
	Deleted   atomic.Int64
}

type Compactor struct {
	cfg   Config
	store *store.Store
	log   *slog.Logger
	stats Stats
}

func New(cfg Config, st *store.Store, log *slog.Logger) *Compactor {
	return &Compactor{cfg: cfg, store: st, log: log}
}

func (c *Compactor) Stats() *Stats { return &c.stats }

func (c *Compactor) Run(ctx context.Context) {
	t := time.NewTicker(c.cfg.Every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := c.RunOnce(ctx); err != nil && ctx.Err() == nil {
				c.log.Error("compaction failed", "err", err)
			}
		}
	}
}

func (c *Compactor) RunOnce(ctx context.Context) error {
	docs, err := c.store.CompactionCandidates(ctx, c.cfg.MinChanges)
	if err != nil {
		return err
	}
	for _, id := range docs {
		if err := c.compact(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (c *Compactor) compact(ctx context.Context, docID string) error {
	snap, changes, err := c.store.Load(ctx, docID)
	if err != nil || len(changes) == 0 {
		return err
	}
	delta := changes[0].Op
	clients := maps.Clone(snap.Clients)
	if clients == nil {
		clients = make(map[string]domain.ClientMark)
	}
	for i, ch := range changes {
		if i > 0 {
			if delta, err = ot.Compose(delta, ch.Op); err != nil {
				return err
			}
		}
		clients[ch.ClientID] = domain.ClientMark{ClientSeq: ch.ClientSeq, Seq: ch.Seq}
	}
	text, err := delta.ApplyString(snap.Text)
	if err != nil {
		return err
	}
	last := changes[len(changes)-1].Seq
	if err := c.store.SaveSnapshot(ctx, domain.Snapshot{DocID: docID, Seq: last, Text: text, Clients: clients}); err != nil {
		return err
	}
	deleted, err := c.store.GC(ctx, docID, last, time.Now().Add(-c.cfg.Retain))
	if err != nil {
		return err
	}
	c.stats.Snapshots.Add(1)
	c.stats.Compacted.Add(int64(len(changes)))
	c.stats.Deleted.Add(deleted)
	return nil
}
