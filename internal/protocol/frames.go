// Package protocol 定義編輯 WebSocket 與 viewer SSE 上的訊息格式。
package protocol

import "collaborative-docs/internal/ot"

// client → server
const (
	TypeJoin   = "join"
	TypeEdit   = "edit"
	TypeCursor = "updateCursor"
)

// server → client
const (
	TypeWelcome = "welcome"
	TypeAck     = "ack"
	TypeOp      = "op"
	TypeCursors = "cursor"
	TypeLeft    = "left"
	TypeMoved   = "moved"
	TypeError   = "error"
)

const (
	CodeDocFull   = "doc_full"
	CodeForbidden = "forbidden"
	CodeStale     = "stale"
	CodeBadEdit   = "bad_edit"
)

// NoRev 表示 client 手上沒有任何版本，需要完整的文件。
const NoRev = -1

type ClientFrame struct {
	Type     string `json:"type"`
	ClientID string `json:"client_id,omitempty"`
	// Rev 是 client 已經套用的最後一個 seq：join 時用來補齊缺少的 change，
	// edit 與 updateCursor 時表示這個編輯或游標是基於哪個版本。
	Rev      int64  `json:"rev"`
	CSeq     int64  `json:"cseq,omitempty"`
	Op       *ot.Op `json:"op,omitempty"`
	Position int    `json:"position,omitempty"`
}

type ServerFrame struct {
	Type  string `json:"type"`
	DocID string `json:"doc_id,omitempty"`
	Seq   int64  `json:"seq,omitempty"`
	// welcome：Text 存在時是完整文件；否則 Changes 是 client 的 rev 之後缺少的 change。
	Text      *string  `json:"text,omitempty"`
	Changes   []Change `json:"changes,omitempty"`
	AckedCSeq int64    `json:"acked_cseq,omitempty"`
	Cursors   []Cursor `json:"cursors,omitempty"`

	CSeq     int64  `json:"cseq,omitempty"`
	ClientID string `json:"client_id,omitempty"`
	UserID   string `json:"user_id,omitempty"`
	Op       *ot.Op `json:"op,omitempty"`
	Position int    `json:"position,omitempty"`

	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type Change struct {
	Seq      int64  `json:"seq"`
	ClientID string `json:"client_id"`
	CSeq     int64  `json:"cseq"`
	UserID   string `json:"user_id"`
	Op       ot.Op  `json:"op"`
}

type Cursor struct {
	ClientID string `json:"client_id"`
	UserID   string `json:"user_id"`
	Position int    `json:"position"`
}

// ViewerEvent 是 SSE 的 data；event 名稱是 snapshot 或 op，id 是 seq（瀏覽器重連時帶在 Last-Event-ID）。
type ViewerEvent struct {
	Seq  int64   `json:"seq"`
	Text *string `json:"text,omitempty"`
	Op   *ot.Op  `json:"op,omitempty"`
}
