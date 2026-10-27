package app

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"collaborative-docs/internal/auth"
	"collaborative-docs/internal/compactor"
	"collaborative-docs/internal/docs"
	"collaborative-docs/internal/domain"
	"collaborative-docs/internal/editor"
	"collaborative-docs/internal/eventbus"
	"collaborative-docs/internal/gateway"
	"collaborative-docs/internal/registry"
	"collaborative-docs/internal/store"
	"collaborative-docs/internal/viewer"
)

type Options struct {
	DBPath     string
	Partitions int
	// LeaseTTL：node 停止 heartbeat 多久之後從名單移除，它的文件交給別人。
	LeaseTTL  time.Duration
	VNodes    int
	Editor    editor.Config
	Viewer    viewer.Config
	Compactor compactor.Config
	Secret    []byte
	Dev       bool
	Logger    *slog.Logger
}

func DefaultOptions(dbPath string, secret []byte) Options {
	return Options{DBPath: dbPath, Partitions: 16, LeaseTTL: time.Second, VNodes: 128,
		Editor: editor.DefaultConfig(), Viewer: viewer.DefaultConfig(), Compactor: compactor.DefaultConfig(),
		Secret: secret, Logger: slog.Default()}
}

// App 組裝所有元件。正式環境裡 gateway、edit node、viewer node、registry、compactor 是各自部署的服務；
// 這裡放在同一個 process，但 node 各有自己的 listener，client 一樣要先問 gateway 再連過去。
type App struct {
	opts      Options
	Store     *store.Store
	Registry  *registry.Registry
	Bus       *eventbus.Topic[domain.Change]
	Docs      *docs.Service
	Signer    *auth.Signer
	Gateway   *gateway.Gateway
	Compactor *compactor.Compactor

	ctx  context.Context
	mu   sync.Mutex
	edit []*editor.Node
	view []*viewer.Node
}

func New(opts Options) (*App, error) {
	st, err := store.Open(opts.DBPath)
	if err != nil {
		return nil, err
	}
	a := &App{opts: opts, Store: st, Registry: registry.New(opts.LeaseTTL), Bus: eventbus.New[domain.Change](opts.Partitions),
		Docs: docs.New(st), Signer: auth.NewSigner(opts.Secret)}
	a.Gateway = gateway.New(gateway.Deps{Signer: a.Signer, Docs: a.Docs, Registry: a.Registry, PartitionOf: a.Bus.PartitionFor,
		VNodes: opts.VNodes, Dev: opts.Dev, Log: opts.Logger.With("component", "gateway")})
	a.Compactor = compactor.New(opts.Compactor, st, opts.Logger.With("component", "compactor"))
	return a, nil
}

func (a *App) Start(ctx context.Context) {
	a.ctx = ctx
	go a.Registry.Run(ctx)
	go a.Compactor.Run(ctx)
	a.Gateway.Run(ctx)
}

// Publish 把 durable 的 change 依 doc id 寫進 event queue，同一份文件的 change 落在同一個分區、保持順序。
func (a *App) Publish(c domain.Change) {
	a.Bus.Append(a.Bus.PartitionFor(c.DocID), c)
}

func (a *App) AddEditNode(ln net.Listener, advertise string) *editor.Node {
	a.mu.Lock()
	defer a.mu.Unlock()
	cfg := a.opts.Editor
	cfg.VNodes = a.opts.VNodes
	n := editor.New(fmt.Sprintf("edit-%d", len(a.edit)), cfg, editor.Deps{Store: a.Store, Docs: a.Docs, Registry: a.Registry,
		Signer: a.Signer, Publisher: a, Log: a.opts.Logger.With("component", "editor")})
	n.Start(a.ctx, ln, advertise)
	a.edit = append(a.edit, n)
	return n
}

func (a *App) AddViewNode(ln net.Listener, advertise string) *viewer.Node {
	a.mu.Lock()
	defer a.mu.Unlock()
	cfg := a.opts.Viewer
	cfg.VNodes = a.opts.VNodes
	n := viewer.New(fmt.Sprintf("view-%d", len(a.view)), cfg, viewer.Deps{Store: a.Store, Docs: a.Docs, Registry: a.Registry,
		Signer: a.Signer, Bus: a.Bus, Log: a.opts.Logger.With("component", "viewer")})
	n.Start(a.ctx, ln, advertise)
	a.view = append(a.view, n)
	return n
}

func (a *App) EditNodes() []*editor.Node {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]*editor.Node(nil), a.edit...)
}

func (a *App) ViewNodes() []*viewer.Node {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]*viewer.Node(nil), a.view...)
}

func (a *App) Close() error {
	return a.Store.Close()
}
