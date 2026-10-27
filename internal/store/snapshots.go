package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"collaborative-docs/internal/domain"
)

// SaveSnapshot 寫入 snapshot 並刪掉更舊的。snapshot 完全由 change log 決定，
// 同一個 seq 不管誰寫都一樣，所以不需要 fencing，重複寫入直接忽略。
func (s *Store) SaveSnapshot(ctx context.Context, snap domain.Snapshot) error {
	clients, err := json.Marshal(snap.Clients)
	if err != nil {
		return err
	}
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO snapshots (doc_id, seq, text, clients, created_at) VALUES (?, ?, ?, ?, ?)`,
		snap.DocID, snap.Seq, snap.Text, string(clients), time.Now().UnixMilli()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM snapshots WHERE doc_id = ? AND seq < (SELECT MAX(seq) FROM snapshots WHERE doc_id = ?)`,
		snap.DocID, snap.DocID); err != nil {
		return err
	}
	return tx.Commit()
}

func latestSnapshot(ctx context.Context, q querier, docID string) (domain.Snapshot, error) {
	snap := domain.Snapshot{DocID: docID}
	var clients string
	err := q.QueryRowContext(ctx, `SELECT seq, text, clients FROM snapshots WHERE doc_id = ? ORDER BY seq DESC LIMIT 1`, docID).
		Scan(&snap.Seq, &snap.Text, &clients)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Snapshot{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Snapshot{}, err
	}
	return snap, json.Unmarshal([]byte(clients), &snap.Clients)
}

// CompactionCandidates 找出最新 snapshot 之後累積了至少 minChanges 個 change 的文件。
func (s *Store) CompactionCandidates(ctx context.Context, minChanges int) ([]string, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT c.doc_id FROM changes c
		JOIN (SELECT doc_id, MAX(seq) AS seq FROM snapshots GROUP BY doc_id) s ON s.doc_id = c.doc_id
		WHERE c.seq > s.seq GROUP BY c.doc_id HAVING COUNT(*) >= ?`, minChanges)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// GC 刪掉已經被 snapshot 涵蓋（seq <= upTo）而且早於 before 的 change。
// 保留一段時間是為了讓短暫斷線的 client 還能用 change 補齊，不必丟掉還沒送出的編輯。
func (s *Store) GC(ctx context.Context, docID string, upTo int64, before time.Time) (int64, error) {
	res, err := s.w.ExecContext(ctx, `DELETE FROM changes WHERE doc_id = ? AND seq <= ? AND created_at < ?`,
		docID, upTo, before.UnixMilli())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// CountChanges 回傳 operation store 目前保存的 change 總數。
func (s *Store) CountChanges(ctx context.Context) (int64, error) {
	var n int64
	err := s.r.QueryRowContext(ctx, `SELECT COUNT(*) FROM changes`).Scan(&n)
	return n, err
}
