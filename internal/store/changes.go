package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"collaborative-docs/internal/domain"
)

var (
	// ErrFenced 表示寫入者已經不是文件目前的 owner。
	ErrFenced = errors.New("document is owned by a newer epoch")
	// ErrCompacted 表示要求的 change 已經被 compaction 清掉，只能改用 snapshot。
	ErrCompacted = errors.New("changes were compacted away")
	ErrClosed    = errors.New("store closed")
)

const maxBatch = 256

type appendReq struct {
	docID   string
	epoch   int64
	changes []domain.Change
	done    func(error)
}

// Claim 讓 node 成為文件的 owner，回傳新的 epoch。之前的 owner 之後的寫入都會得到 ErrFenced。
// 它和 change 的寫入共用同一條 writer 連線，所以 claim 之前 commit 的 change 一定在 claim 之後讀得到。
func (s *Store) Claim(ctx context.Context, docID, node string) (int64, error) {
	var epoch int64
	err := s.w.QueryRowContext(ctx, `UPDATE docs SET owner_node = ?, owner_epoch = owner_epoch + 1 WHERE doc_id = ? RETURNING owner_epoch`,
		node, docID).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, domain.ErrNotFound
	}
	return epoch, err
}

// AppendAsync 排入一筆寫入，commit 之後呼叫 done。done 依排入順序被呼叫，
// 同一份文件的 change 因此依 seq 順序變成 durable。
func (s *Store) AppendAsync(docID string, epoch int64, changes []domain.Change, done func(error)) {
	select {
	case s.appends <- appendReq{docID: docID, epoch: epoch, changes: changes, done: done}:
	case <-s.done:
		done(ErrClosed)
	}
}

func (s *Store) writeLoop() {
	defer s.wg.Done()
	for {
		var first appendReq
		select {
		case first = <-s.appends:
		case <-s.done:
			return
		}
		batch := []appendReq{first}
	drain:
		for len(batch) < maxBatch {
			select {
			case r := <-s.appends:
				batch = append(batch, r)
			default:
				break drain
			}
		}
		errs := s.commit(batch)
		for i, r := range batch {
			r.done(errs[i])
		}
	}
}

// commit 把整批寫在一個交易裡。每一筆用 savepoint 隔開：某份文件被 fence 只撤銷那一筆，其他文件照常 commit。
func (s *Store) commit(batch []appendReq) []error {
	errs := make([]error, len(batch))
	failAll := func(err error) []error {
		for i := range errs {
			errs[i] = err
		}
		return errs
	}
	ctx := context.Background()
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return failAll(err)
	}
	defer tx.Rollback()
	epochQ, err := tx.PrepareContext(ctx, `SELECT owner_epoch FROM docs WHERE doc_id = ?`)
	if err != nil {
		return failAll(err)
	}
	insertQ, err := tx.PrepareContext(ctx, `INSERT INTO changes (doc_id, seq, client_id, client_seq, user_id, op, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return failAll(err)
	}
	n := 0
	for i, r := range batch {
		if _, err := tx.ExecContext(ctx, `SAVEPOINT append`); err != nil {
			return failAll(err)
		}
		errs[i] = appendOne(ctx, epochQ, insertQ, r)
		if errs[i] != nil {
			if _, err := tx.ExecContext(ctx, `ROLLBACK TO append`); err != nil {
				return failAll(err)
			}
		} else {
			n += len(r.changes)
		}
		if _, err := tx.ExecContext(ctx, `RELEASE append`); err != nil {
			return failAll(err)
		}
	}
	if err := tx.Commit(); err != nil {
		return failAll(err)
	}
	s.batches.Add(1)
	s.appended.Add(int64(n))
	return errs
}

func appendOne(ctx context.Context, epochQ, insertQ *sql.Stmt, r appendReq) error {
	var epoch int64
	if err := epochQ.QueryRowContext(ctx, r.docID).Scan(&epoch); err != nil {
		return err
	}
	if epoch != r.epoch {
		return ErrFenced
	}
	for _, c := range r.changes {
		op, err := json.Marshal(c.Op)
		if err != nil {
			return err
		}
		if _, err := insertQ.ExecContext(ctx, r.docID, c.Seq, c.ClientID, c.ClientSeq, c.UserID, string(op), c.At.UnixMilli()); err != nil {
			return err
		}
	}
	return nil
}

// Load 回傳最新的 snapshot 與它之後的所有 change。兩個查詢在同一個讀取交易裡，
// 不會讀到「舊 snapshot 配上已經被 compaction 清掉的 change」。
func (s *Store) Load(ctx context.Context, docID string) (domain.Snapshot, []domain.Change, error) {
	tx, err := s.r.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return domain.Snapshot{}, nil, err
	}
	defer tx.Rollback()
	snap, err := latestSnapshot(ctx, tx, docID)
	if err != nil {
		return domain.Snapshot{}, nil, err
	}
	changes, err := changesBetween(ctx, tx, docID, snap.Seq, math.MaxInt64)
	if err != nil {
		return domain.Snapshot{}, nil, err
	}
	for i, c := range changes {
		if c.Seq != snap.Seq+1+int64(i) {
			return domain.Snapshot{}, nil, fmt.Errorf("doc %s: change %d missing after snapshot %d", docID, snap.Seq+1+int64(i), snap.Seq)
		}
	}
	return snap, changes, nil
}

// ChangesSince 回傳 after < seq <= upTo 的 change；其中有任何一個已經被清掉時回傳 ErrCompacted。
func (s *Store) ChangesSince(ctx context.Context, docID string, after, upTo int64) ([]domain.Change, error) {
	changes, err := changesBetween(ctx, s.r, docID, after, upTo)
	if err != nil {
		return nil, err
	}
	if int64(len(changes)) != upTo-after || (len(changes) > 0 && changes[0].Seq != after+1) {
		return nil, ErrCompacted
	}
	return changes, nil
}

// Head 回傳文件目前最新的 seq。
func (s *Store) Head(ctx context.Context, docID string) (int64, error) {
	var head int64
	err := s.r.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM (
		SELECT MAX(seq) AS seq FROM changes WHERE doc_id = ?
		UNION ALL SELECT MAX(seq) FROM snapshots WHERE doc_id = ?)`, docID, docID).Scan(&head)
	return head, err
}

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func changesBetween(ctx context.Context, q querier, docID string, after, upTo int64) ([]domain.Change, error) {
	rows, err := q.QueryContext(ctx, `SELECT seq, client_id, client_seq, user_id, op, created_at FROM changes
		WHERE doc_id = ? AND seq > ? AND seq <= ? ORDER BY seq`, docID, after, upTo)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Change
	for rows.Next() {
		c := domain.Change{DocID: docID}
		var op string
		var at int64
		if err := rows.Scan(&c.Seq, &c.ClientID, &c.ClientSeq, &c.UserID, &op, &at); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(op), &c.Op); err != nil {
			return nil, fmt.Errorf("doc %s change %d: %w", docID, c.Seq, err)
		}
		c.At = time.UnixMilli(at)
		out = append(out, c)
	}
	return out, rows.Err()
}
