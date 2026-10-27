package editor

import (
	"context"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"collaborative-docs/internal/protocol"
)

// conn 是一條編輯者的 WebSocket。session 只透過 send 寫入，send 永遠不會阻塞。
type conn struct {
	ws       *websocket.Conn
	userID   string
	clientID string

	mu     sync.Mutex
	out    chan protocol.ServerFrame
	closed bool
}

func newConn(ws *websocket.Conn, userID string, buffer int) *conn {
	return &conn{ws: ws, userID: userID, out: make(chan protocol.ServerFrame, buffer)}
}

func (c *conn) send(f protocol.ServerFrame) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	select {
	case c.out <- f:
	default:
		// client 跟不上就斷線，讓它重連後用 rev 補齊，不讓一個慢的 client 拖住整份文件。
		c.closed = true
		close(c.out)
	}
}

// finish 送出最後一個 frame 後關閉連線。
func (c *conn) finish(f protocol.ServerFrame) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	select {
	case c.out <- f:
	default:
	}
	close(c.out)
}

func (c *conn) writeLoop(ctx context.Context) {
	for f := range c.out {
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := wsjson.Write(wctx, c.ws, f)
		cancel()
		if err != nil {
			c.ws.CloseNow()
			return
		}
	}
	c.ws.Close(websocket.StatusNormalClosure, "")
}

func (c *conn) readLoop(ctx context.Context, s *session) {
	for {
		var f protocol.ClientFrame
		if err := wsjson.Read(ctx, c.ws, &f); err != nil {
			return
		}
		switch f.Type {
		case protocol.TypeEdit:
			if f.Op == nil || !s.post(editMsg{c: c, cseq: f.CSeq, rev: f.Rev, op: *f.Op}) {
				return
			}
		case protocol.TypeCursor:
			if !s.post(cursorMsg{c: c, rev: f.Rev, pos: f.Position}) {
				return
			}
		default:
			return
		}
	}
}

func movedFrame() protocol.ServerFrame {
	return protocol.ServerFrame{Type: protocol.TypeMoved}
}

func errorFrame(code, msg string) protocol.ServerFrame {
	return protocol.ServerFrame{Type: protocol.TypeError, Code: code, Message: msg}
}
