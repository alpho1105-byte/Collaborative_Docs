package client

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"collaborative-docs/internal/ot"
	"collaborative-docs/internal/protocol"
)

// testServer 用和 editor session 相同的規則定序：把編輯轉換到最新版本、指派 seq、
// 以 (client, cseq) 擋下重送。
type testServer struct {
	t     *testing.T
	text  []rune
	log   []protocol.Change
	marks map[string]int64
}

func (s *testServer) receive(clientID string, f protocol.ClientFrame) (protocol.Change, bool) {
	if f.CSeq <= s.marks[clientID] {
		return protocol.Change{}, false
	}
	op := *f.Op
	for _, c := range s.log[f.Rev:] {
		var err error
		if op, _, err = ot.Transform(op, c.Op); err != nil {
			s.t.Fatal(err)
		}
	}
	text, err := op.Apply(s.text)
	if err != nil {
		s.t.Fatalf("server apply: %v", err)
	}
	s.text = text
	c := protocol.Change{Seq: int64(len(s.log) + 1), ClientID: clientID, CSeq: f.CSeq, Op: op}
	s.log = append(s.log, c)
	s.marks[clientID] = f.CSeq
	return c, true
}

func (s *testServer) welcome(clientID string, rev int64) protocol.ServerFrame {
	f := protocol.ServerFrame{Type: protocol.TypeWelcome, Seq: int64(len(s.log)), AckedCSeq: s.marks[clientID]}
	if rev < 0 {
		text := string(s.text)
		f.Text = &text
		return f
	}
	f.Changes = append(f.Changes, s.log[rev:]...)
	return f
}

type testClient struct {
	doc *Doc
	in  []protocol.ServerFrame
	out []protocol.ClientFrame
}

func (c *testClient) send(f *protocol.ClientFrame) {
	if f != nil {
		c.out = append(c.out, *f)
	}
}

// 隨機交錯本地編輯、訊息送達與斷線重連，最後所有 client 必須和 server 的文件完全相同，
// 而且每個編輯都只被套用一次（以每個 client 插入的唯一字元檢查）。
func TestRandomInterleavingsConvergeThroughReconnects(t *testing.T) {
	for seed := range uint64(40) {
		r := rand.New(rand.NewPCG(seed, 7))
		srv := &testServer{t: t, marks: map[string]int64{}}
		clients := make([]*testClient, 4)
		typed := 0
		for i := range clients {
			clients[i] = &testClient{doc: NewDoc(fmt.Sprintf("c%d", i))}
			resend, _, _, err := clients[i].doc.Welcome(srv.welcome(clients[i].doc.ClientID(), protocol.NoRev))
			if err != nil {
				t.Fatal(err)
			}
			clients[i].send(resend)
		}
		deliver := func(c *testClient) {
			f := c.in[0]
			c.in = c.in[1:]
			var err error
			switch f.Type {
			case protocol.TypeAck:
				_, err = c.doc.Ack(f.CSeq, f.Seq)
			case protocol.TypeOp:
				_, err = c.doc.Apply(protocol.Change{Seq: f.Seq, ClientID: f.ClientID, CSeq: f.CSeq, Op: *f.Op})
			}
			if err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
			c.send(c.doc.Next())
		}
		process := func(c *testClient) {
			f := c.out[0]
			c.out = c.out[1:]
			change, ok := srv.receive(c.doc.ClientID(), f)
			if !ok {
				return
			}
			for _, other := range clients {
				if other == c {
					other.in = append(other.in, protocol.ServerFrame{Type: protocol.TypeAck, CSeq: change.CSeq, Seq: change.Seq})
				} else {
					op := change.Op
					other.in = append(other.in, protocol.ServerFrame{Type: protocol.TypeOp, Seq: change.Seq, ClientID: change.ClientID, CSeq: change.CSeq, Op: &op})
				}
			}
		}
		for step := 0; step < 3000; step++ {
			c := clients[r.IntN(len(clients))]
			switch x := r.IntN(100); {
			case x < 30:
				typed++
				mark := string(rune(0x4e00 + typed)) // 每次插入一個獨一無二的字
				if err := c.doc.Insert(r.IntN(c.doc.Len()+1), mark); err != nil {
					t.Fatal(err)
				}
				c.send(c.doc.Next())
			case x < 35 && c.doc.Len() > 0:
				pos := r.IntN(c.doc.Len())
				if err := c.doc.Delete(pos, 1+r.IntN(min(2, c.doc.Len()-pos))); err != nil {
					t.Fatal(err)
				}
				c.send(c.doc.Next())
			case x < 65 && len(c.out) > 0:
				process(c)
			case x < 97 && len(c.in) > 0:
				deliver(c)
			case x >= 97:
				// 斷線：路上的訊息全部遺失，重連後從 rev 補齊並重送 outstanding。
				c.in, c.out = nil, nil
				c.doc.SetOffline()
				resend, _, discarded, err := c.doc.Welcome(srv.welcome(c.doc.ClientID(), c.doc.Rev()))
				if err != nil || discarded {
					t.Fatalf("seed %d: reconnect: %v discarded=%v", seed, err, discarded)
				}
				c.send(resend)
			}
		}
		for busy := true; busy; {
			busy = false
			for _, c := range clients {
				for len(c.out) > 0 {
					process(c)
					busy = true
				}
				for len(c.in) > 0 {
					deliver(c)
					busy = true
				}
			}
		}
		for _, c := range clients {
			if !c.doc.Synced() || c.doc.Text() != string(srv.text) {
				t.Fatalf("seed %d: client %s has %q (synced=%v), server has %q", seed, c.doc.ClientID(), c.doc.Text(), c.doc.Synced(), string(srv.text))
			}
		}
		seen := map[rune]int{}
		for _, ch := range srv.text {
			seen[ch]++
		}
		for ch, n := range seen {
			if n > 1 {
				t.Fatalf("seed %d: %q appears %d times: an edit was applied twice", seed, ch, n)
			}
		}
	}
}

func TestSnapshotWelcomeDiscardsOnlyEditsTheServerNeverApplied(t *testing.T) {
	text := "hello"
	d := NewDoc("c1")
	d.Welcome(protocol.ServerFrame{Seq: 3, Text: &text})
	d.Insert(5, "!")
	d.Next()
	d.SetOffline()

	applied := "hello!"
	if _, acked, discarded, _ := d.Welcome(protocol.ServerFrame{Seq: 4, Text: &applied, AckedCSeq: 1}); discarded || acked != 1 || !d.Synced() {
		t.Fatalf("applied edit: acked=%d discarded=%v", acked, discarded)
	}
	d.Insert(6, "?")
	d.Next()
	d.SetOffline()
	if _, _, discarded, _ := d.Welcome(protocol.ServerFrame{Seq: 9, Text: &applied, AckedCSeq: 1}); !discarded || d.Text() != applied {
		t.Fatalf("unapplied edit: discarded=%v text=%q", discarded, d.Text())
	}
}
