package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"collaborative-docs/internal/domain"
)

// CreateDoc 建立文件、把建立者設為 owner，並寫入 seq 0 的空白 snapshot，之後載入文件一律從 snapshot 開始。
func (s *Store) CreateDoc(ctx context.Context, d domain.Doc) error {
	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := d.CreatedAt.UnixMilli()
	if _, err := tx.ExecContext(ctx, `INSERT INTO docs (doc_id, title, created_by, created_at) VALUES (?, ?, ?, ?)`,
		d.ID, d.Title, d.CreatedBy, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO members (doc_id, user_id, role) VALUES (?, ?, ?)`,
		d.ID, d.CreatedBy, domain.RoleOwner); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO snapshots (doc_id, seq, text, clients, created_at) VALUES (?, 0, '', '{}', ?)`,
		d.ID, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) GetDoc(ctx context.Context, id string) (domain.Doc, error) {
	var d domain.Doc
	var created int64
	err := s.r.QueryRowContext(ctx, `SELECT doc_id, title, created_by, created_at FROM docs WHERE doc_id = ?`, id).
		Scan(&d.ID, &d.Title, &d.CreatedBy, &created)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Doc{}, domain.ErrNotFound
	}
	d.CreatedAt = time.UnixMilli(created)
	return d, err
}

func (s *Store) SetMember(ctx context.Context, docID, userID string, role domain.Role) error {
	_, err := s.w.ExecContext(ctx, `INSERT INTO members (doc_id, user_id, role) VALUES (?, ?, ?)
		ON CONFLICT (doc_id, user_id) DO UPDATE SET role = excluded.role`, docID, userID, role)
	return err
}

// Role 回傳使用者在文件中的角色；不是成員或文件不存在時回傳 ErrNotFound。
func (s *Store) Role(ctx context.Context, docID, userID string) (domain.Role, error) {
	var role domain.Role
	err := s.r.QueryRowContext(ctx, `SELECT role FROM members WHERE doc_id = ? AND user_id = ?`, docID, userID).Scan(&role)
	if errors.Is(err, sql.ErrNoRows) {
		return "", domain.ErrNotFound
	}
	return role, err
}
