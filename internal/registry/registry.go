// Package registry 模擬 ZooKeeper / Consul 這類 service registry：node 上線時註冊、
// 以 heartbeat 維持租約，租約過期就從成員名單移除。名單每變一次版本號加一，並推送給訂閱者，
// gateway 與 node 拿同一份名單在本地算 consistent hashing。
package registry

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	KindEdit = "edit"
	KindView = "view"
)

type Node struct {
	ID   string
	Kind string
	Addr string
}

type Membership struct {
	Version int64
	Nodes   []Node // 依 ID 排序
}

func (m Membership) IDs() []string {
	ids := make([]string, len(m.Nodes))
	for i, n := range m.Nodes {
		ids[i] = n.ID
	}
	return ids
}

func (m Membership) Addr(id string) (string, bool) {
	for _, n := range m.Nodes {
		if n.ID == id {
			return n.Addr, true
		}
	}
	return "", false
}

type member struct {
	node     Node
	lastBeat time.Time
}

type Registry struct {
	mu       sync.Mutex
	ttl      time.Duration
	members  map[string]*member
	version  int64
	watchers map[string][]chan Membership
}

func New(ttl time.Duration) *Registry {
	return &Registry{ttl: ttl, members: make(map[string]*member), watchers: make(map[string][]chan Membership)}
}

func (r *Registry) Register(n Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.members[n.ID] = &member{node: n, lastBeat: time.Now()}
	r.publish(n.Kind)
}

// Heartbeat 延長租約。回傳 false 表示租約已經過期：node 必須放下手上所有文件再重新註冊，
// 因為這段期間它的文件可能已經交給別人。
func (r *Registry) Heartbeat(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.members[id]
	if !ok {
		return false
	}
	m.lastBeat = time.Now()
	return true
}

func (r *Registry) Deregister(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.members[id]; ok {
		delete(r.members, id)
		r.publish(m.node.Kind)
	}
}

// Watch 回傳某一類 node 的名單更新。channel 只保留最新的一份，消費者慢也不會讓 registry 卡住。
func (r *Registry) Watch(kind string) <-chan Membership {
	r.mu.Lock()
	defer r.mu.Unlock()
	ch := make(chan Membership, 1)
	ch <- r.membership(kind)
	r.watchers[kind] = append(r.watchers[kind], ch)
	return ch
}

func (r *Registry) Members(kind string) Membership {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.membership(kind)
}

// Run 定期移除租約過期的 node，直到 ctx 結束。
func (r *Registry) Run(ctx context.Context) {
	ticker := time.NewTicker(r.ttl / 5)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.expire()
		}
	}
}

func (r *Registry) expire() {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := time.Now().Add(-r.ttl)
	changed := map[string]bool{}
	for id, m := range r.members {
		if m.lastBeat.Before(cutoff) {
			delete(r.members, id)
			changed[m.node.Kind] = true
		}
	}
	for kind := range changed {
		r.publish(kind)
	}
}

func (r *Registry) publish(kind string) {
	r.version++
	m := r.membership(kind)
	for _, ch := range r.watchers[kind] {
		select {
		case <-ch:
		default:
		}
		ch <- m
	}
}

func (r *Registry) membership(kind string) Membership {
	m := Membership{Version: r.version}
	for _, mem := range r.members {
		if mem.node.Kind == kind {
			m.Nodes = append(m.Nodes, mem.node)
		}
	}
	slices.SortFunc(m.Nodes, func(a, b Node) int { return strings.Compare(a.ID, b.ID) })
	return m
}
