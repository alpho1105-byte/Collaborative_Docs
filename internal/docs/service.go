// Package docs 是文件的 CRUD：建立文件、查詢、分享給其他使用者。
package docs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"collaborative-docs/internal/domain"
	"collaborative-docs/internal/store"
)

var ErrInvalid = errors.New("invalid input")

const maxTitle = 200

type Service struct {
	store *store.Store
}

func New(st *store.Store) *Service {
	return &Service{store: st}
}

func (s *Service) Create(ctx context.Context, userID, title string) (domain.Doc, error) {
	title = strings.TrimSpace(title)
	if title == "" || utf8.RuneCountInString(title) > maxTitle {
		return domain.Doc{}, ErrInvalid
	}
	b := make([]byte, 8)
	rand.Read(b)
	d := domain.Doc{ID: "doc_" + hex.EncodeToString(b), Title: title, CreatedBy: userID, CreatedAt: time.Now()}
	return d, s.store.CreateDoc(ctx, d)
}

// Get 回傳文件與使用者的角色。不是成員的人一律當作文件不存在，不透露文件 id 是否有效。
func (s *Service) Get(ctx context.Context, userID, docID string) (domain.Doc, domain.Role, error) {
	role, err := s.store.Role(ctx, docID, userID)
	if err != nil {
		return domain.Doc{}, "", err
	}
	d, err := s.store.GetDoc(ctx, docID)
	return d, role, err
}

// Share 讓 owner 把文件分享給其他人當 editor 或 viewer。
func (s *Service) Share(ctx context.Context, userID, docID, target string, role domain.Role) error {
	if target == "" || (role != domain.RoleEditor && role != domain.RoleViewer) {
		return ErrInvalid
	}
	mine, err := s.store.Role(ctx, docID, userID)
	if err != nil {
		return err
	}
	if mine != domain.RoleOwner {
		return domain.ErrForbidden
	}
	if target == userID {
		return ErrInvalid
	}
	return s.store.SetMember(ctx, docID, target, role)
}

func (s *Service) Role(ctx context.Context, docID, userID string) (domain.Role, error) {
	return s.store.Role(ctx, docID, userID)
}
