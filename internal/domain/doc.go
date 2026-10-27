package domain

import (
	"errors"
	"time"

	"collaborative-docs/internal/ot"
)

var (
	ErrNotFound  = errors.New("not found")
	ErrForbidden = errors.New("forbidden")
)

type Role string

const (
	RoleOwner  Role = "owner"
	RoleEditor Role = "editor"
	RoleViewer Role = "viewer"
)

func (r Role) Valid() bool   { return r == RoleOwner || r == RoleEditor || r == RoleViewer }
func (r Role) CanEdit() bool { return r == RoleOwner || r == RoleEditor }

type Doc struct {
	ID        string
	Title     string
	CreatedBy string
	CreatedAt time.Time
}

// Change 是一個已經定序的編輯。Seq 由文件的 owner 指派，從 1 開始連續遞增，
// 所有 client 與 viewer 都依 Seq 套用，Op 是轉換後、直接套用在 Seq-1 版本上的編輯。
type Change struct {
	DocID     string
	Seq       int64
	ClientID  string
	ClientSeq int64
	UserID    string
	Op        ot.Op
	At        time.Time
}

// ClientMark 記錄某個 client 最後一個被套用的編輯，用來辨認斷線重連後重送的編輯。
type ClientMark struct {
	ClientSeq int64 `json:"cseq"`
	Seq       int64 `json:"seq"`
}

// Snapshot 是文件在 Seq 版本的全文。載入文件只需要最新的 snapshot 加上之後的 change。
type Snapshot struct {
	DocID   string
	Seq     int64
	Text    string
	Clients map[string]ClientMark
}
