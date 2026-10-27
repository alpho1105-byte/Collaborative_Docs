package client

import (
	"errors"
	"fmt"

	"collaborative-docs/internal/ot"
	"collaborative-docs/internal/protocol"
)

// ErrOutOfSync 表示收到的訊息和本地狀態對不上（例如 seq 有缺口），需要重新連線同步。
var ErrOutOfSync = errors.New("client is out of sync with the server")

type pending struct {
	cseq int64
	op   ot.Op
}

// Doc 是 client 端的 OT 狀態機，不含網路 I/O。
//
// 本地文件 = server 在 rev 版本的文件，再套用 outstanding（已送出、等 ack 的編輯）與
// buffer（還沒送出的編輯，連續按鍵 compose 成一個）。同一時間最多一個 outstanding，
// server 因此只需要對每個編輯做一次轉換。
type Doc struct {
	clientID    string
	text        []rune
	rev         int64
	online      bool
	outstanding *pending
	buffer      *ot.Op
	nextCSeq    int64
	cursor      int
	cursorDirty bool
	// remote 是其他協作者的游標，以 server 在 rev 版本的座標保存。
	remote map[string]protocol.Cursor
}

func NewDoc(clientID string) *Doc {
	return &Doc{clientID: clientID, rev: protocol.NoRev, remote: make(map[string]protocol.Cursor)}
}

func (d *Doc) ClientID() string { return d.clientID }
func (d *Doc) Text() string     { return string(d.text) }
func (d *Doc) Len() int         { return len(d.text) }
func (d *Doc) Rev() int64       { return d.rev }
func (d *Doc) Cursor() int      { return d.cursor }
func (d *Doc) Online() bool     { return d.online }

// Synced 表示沒有任何還沒被 server 確認的本地編輯。
func (d *Doc) Synced() bool { return d.outstanding == nil && d.buffer == nil }

// Remote 回傳其他協作者的游標（server 座標）。
func (d *Doc) Remote() map[string]protocol.Cursor {
	out := make(map[string]protocol.Cursor, len(d.remote))
	for id, c := range d.remote {
		out[id] = c
	}
	return out
}

func (d *Doc) Insert(pos int, s string) error {
	op, err := ot.NewInsert(len(d.text), pos, s)
	if err != nil {
		return err
	}
	return d.local(op)
}

func (d *Doc) Delete(pos, n int) error {
	op, err := ot.NewDelete(len(d.text), pos, n)
	if err != nil {
		return err
	}
	return d.local(op)
}

func (d *Doc) SetCursor(pos int) {
	d.cursor = max(0, min(pos, len(d.text)))
	d.cursorDirty = true
}

// local 立即套用本地編輯（使用者不必等 server），再併進 buffer 等待送出。
func (d *Doc) local(op ot.Op) error {
	text, err := op.Apply(d.text)
	if err != nil {
		return err
	}
	buffer := op
	if d.buffer != nil {
		if buffer, err = ot.Compose(*d.buffer, op); err != nil {
			return err
		}
	}
	d.text = text
	d.buffer = &buffer
	d.cursor = ot.TransformIndex(d.cursor, op, true)
	d.cursorDirty = true
	return nil
}

// Next 在可以送出時把 buffer 變成 outstanding，回傳要送出的 edit。
func (d *Doc) Next() *protocol.ClientFrame {
	if !d.online || d.outstanding != nil || d.buffer == nil {
		return nil
	}
	d.nextCSeq++
	d.outstanding = &pending{cseq: d.nextCSeq, op: *d.buffer}
	d.buffer = nil
	return d.editFrame()
}

func (d *Doc) editFrame() *protocol.ClientFrame {
	op := d.outstanding.op
	return &protocol.ClientFrame{Type: protocol.TypeEdit, CSeq: d.outstanding.cseq, Rev: d.rev, Op: &op}
}

// Ack 處理自己的編輯被 server 套用在 seq。回傳被確認的 cseq，重複的 ack 回傳 0。
func (d *Doc) Ack(cseq, seq int64) (int64, error) {
	if d.outstanding == nil || cseq != d.outstanding.cseq {
		return 0, nil
	}
	if seq != d.rev+1 {
		return 0, fmt.Errorf("%w: ack for seq %d at rev %d", ErrOutOfSync, seq, d.rev)
	}
	// server 套用的就是轉換到最新的 outstanding，游標跟著它移動。
	for id, c := range d.remote {
		c.Position = ot.TransformIndex(c.Position, d.outstanding.op, false)
		d.remote[id] = c
	}
	d.rev = seq
	d.outstanding = nil
	return cseq, nil
}

// Apply 套用 server 定序過的 change。別人的編輯要先對 outstanding 與 buffer 做轉換才能套用在本地文件上：
// server 的順序是「它先、我後」，本地卻是我的編輯先套用了。
func (d *Doc) Apply(c protocol.Change) (int64, error) {
	if c.Seq <= d.rev {
		return 0, nil
	}
	if c.Seq != d.rev+1 {
		return 0, fmt.Errorf("%w: change %d at rev %d", ErrOutOfSync, c.Seq, d.rev)
	}
	if c.ClientID == d.clientID {
		// 連線中斷又重連時，自己的編輯可能以 change 的形式送回來，等同於 ack。
		if d.outstanding != nil && c.CSeq == d.outstanding.cseq {
			return d.Ack(c.CSeq, c.Seq)
		}
		return 0, fmt.Errorf("%w: own change %d is not the outstanding edit", ErrOutOfSync, c.CSeq)
	}
	op := c.Op
	outstanding, buffer := d.outstanding, d.buffer
	if outstanding != nil {
		o, rest, err := ot.Transform(outstanding.op, op)
		if err != nil {
			return 0, err
		}
		outstanding, op = &pending{cseq: outstanding.cseq, op: o}, rest
	}
	if buffer != nil {
		b, rest, err := ot.Transform(*buffer, op)
		if err != nil {
			return 0, err
		}
		buffer, op = &b, rest
	}
	text, err := op.Apply(d.text)
	if err != nil {
		return 0, err
	}
	d.text, d.outstanding, d.buffer = text, outstanding, buffer
	d.cursor = ot.TransformIndex(d.cursor, op, false)
	for id, cur := range d.remote {
		cur.Position = ot.TransformIndex(cur.Position, c.Op, id == c.ClientID)
		d.remote[id] = cur
	}
	d.rev = c.Seq
	return 0, nil
}

// Welcome 處理（重新）連線後的第一個訊息。回傳要重送的 edit、其中確認的 cseq，
// 以及是否因為 server 只能給全文而丟掉了還沒確認的本地編輯。
func (d *Doc) Welcome(f protocol.ServerFrame) (*protocol.ClientFrame, int64, bool, error) {
	var acked int64
	discarded := false
	if f.Text != nil {
		// 全文無法和本地還沒確認的編輯合併；outstanding 已經被套用（cseq <= acked_cseq）時不算遺失。
		if d.buffer != nil || (d.outstanding != nil && d.outstanding.cseq > f.AckedCSeq) {
			discarded = true
		} else if d.outstanding != nil {
			acked = d.outstanding.cseq
		}
		d.outstanding, d.buffer = nil, nil
		d.text, d.rev = []rune(*f.Text), f.Seq
		d.cursor = min(d.cursor, len(d.text))
	} else {
		for _, c := range f.Changes {
			a, err := d.Apply(c)
			if err != nil {
				return nil, 0, false, err
			}
			acked = max(acked, a)
		}
		if d.rev != f.Seq {
			return nil, 0, false, fmt.Errorf("%w: welcome at seq %d left the client at rev %d", ErrOutOfSync, f.Seq, d.rev)
		}
	}
	d.remote = make(map[string]protocol.Cursor, len(f.Cursors))
	for _, c := range f.Cursors {
		if c.ClientID != d.clientID {
			d.remote[c.ClientID] = c
		}
	}
	d.online = true
	d.cursorDirty = true
	if d.outstanding != nil {
		// 重送同一個 cseq：它已經依補上的 change 轉換過，基於新的 rev。
		return d.editFrame(), acked, discarded, nil
	}
	return d.Next(), acked, discarded, nil
}

func (d *Doc) SetOffline() { d.online = false }

// CursorFrame 只在沒有待確認的編輯時回報游標：此時本地座標就等於 server 在 rev 版本的座標。
func (d *Doc) CursorFrame() *protocol.ClientFrame {
	if !d.online || !d.cursorDirty || !d.Synced() {
		return nil
	}
	d.cursorDirty = false
	return &protocol.ClientFrame{Type: protocol.TypeCursor, Rev: d.rev, Position: d.cursor}
}

func (d *Doc) SetRemoteCursor(c protocol.Cursor) {
	if c.ClientID != d.clientID {
		d.remote[c.ClientID] = c
	}
}

func (d *Doc) RemoveRemote(clientID string) {
	delete(d.remote, clientID)
}
