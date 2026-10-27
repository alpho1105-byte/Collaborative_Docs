package viewer

import (
	"sync"
	"time"

	"collaborative-docs/internal/domain"
	"collaborative-docs/internal/protocol"
)

type event struct {
	name string
	data protocol.ViewerEvent
}

type subscriber struct {
	ch chan event
}

// docState 是一份文件在 viewer-owner 上的狀態：全文、最新 seq 與最近的 change，
// 只在有人觀看時存在。所有欄位由 mu 保護。
type docState struct {
	id   string
	part int

	mu        sync.Mutex
	loaded    bool
	removed   bool
	text      []rune
	seq       int64
	tail      []domain.Change
	subs      map[*subscriber]struct{}
	lastEvent time.Time
}

func (d *docState) apply(c domain.Change, tailLen int) error {
	text, err := c.Op.Apply(d.text)
	if err != nil {
		return err
	}
	d.text, d.seq, d.lastEvent = text, c.Seq, time.Now()
	d.tail = append(d.tail, c)
	if len(d.tail) > 2*tailLen {
		d.tail = append([]domain.Change(nil), d.tail[len(d.tail)-tailLen:]...)
	}
	op := c.Op
	d.push(event{name: "op", data: protocol.ViewerEvent{Seq: c.Seq, Op: &op}})
	return nil
}

func (d *docState) snapshotEvent() event {
	text := string(d.text)
	return event{name: "snapshot", data: protocol.ViewerEvent{Seq: d.seq, Text: &text}}
}

// push 不會阻塞：跟不上的 viewer 直接斷線，重連時用 Last-Event-ID 補齊。
func (d *docState) push(e event) {
	for s := range d.subs {
		select {
		case s.ch <- e:
		default:
			d.drop(s)
		}
	}
}

func (d *docState) drop(s *subscriber) {
	if _, ok := d.subs[s]; ok {
		delete(d.subs, s)
		close(s.ch)
	}
}

func (d *docState) closeAll() {
	for s := range d.subs {
		d.drop(s)
	}
}

// subscribe 註冊 viewer 並回傳要先送出的事件：resume 之後的 change 還在 tail 裡就只補那一段，
// 否則送目前的全文。
func (d *docState) subscribe(resume int64, buffer int) (*subscriber, []event, bool) {
	s := &subscriber{ch: make(chan event, buffer)}
	d.subs[s] = struct{}{}
	if resume >= 0 && resume <= d.seq && (resume == d.seq || (len(d.tail) > 0 && d.tail[0].Seq <= resume+1)) {
		var initial []event
		for _, c := range d.tail {
			if c.Seq > resume {
				op := c.Op
				initial = append(initial, event{name: "op", data: protocol.ViewerEvent{Seq: c.Seq, Op: &op}})
			}
		}
		return s, initial, true
	}
	return s, []event{d.snapshotEvent()}, false
}
