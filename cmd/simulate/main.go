// simulate 在本機啟動完整的叢集（gateway、edit node、viewer node、registry、compactor），
// 讓幾百個編輯者與觀看者透過真正的 WebSocket / SSE 同時編輯，途中加入 node、讓 node 當機、
// 製造被 registry 隔離但仍在服務的 zombie node，最後驗證所有人看到的文件完全一致、
// 每個編輯恰好套用一次、游標位置一致。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"time"

	"collaborative-docs/internal/app"
	"collaborative-docs/internal/client"
	"collaborative-docs/internal/domain"
	"collaborative-docs/internal/ot"
)

const (
	hotEditors = 100
	hotViewers = 40
	leaseTTL   = 600 * time.Millisecond
	pauseFor   = 2500 * time.Millisecond
)

func main() {
	seed := flag.Uint64("seed", 42, "random seed")
	docs := flag.Int("docs", 40, "number of documents (the first one is the hot document)")
	duration := flag.Duration("duration", 16*time.Second, "how long the editors keep typing")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	if err := run(*seed, *docs, *duration, logger); err != nil {
		fmt.Println("error:", err)
		os.Exit(1)
	}
	if failures > 0 {
		fmt.Printf("\n%d check(s) failed\n", failures)
		os.Exit(1)
	}
}

func simOptions(dbPath string, logger *slog.Logger) app.Options {
	opts := app.DefaultOptions(dbPath, []byte("sim-secret"))
	opts.Dev = true
	opts.Logger = logger
	opts.LeaseTTL = leaseTTL
	opts.Editor.Heartbeat = 100 * time.Millisecond
	opts.Editor.Window = 256
	opts.Viewer.Heartbeat = 100 * time.Millisecond
	opts.Viewer.Tail = 64
	opts.Viewer.Reconcile = 500 * time.Millisecond
	opts.Compactor.Every = 500 * time.Millisecond
	opts.Compactor.MinChanges = 100
	opts.Compactor.Retain = 5 * time.Second
	return opts
}

type sim struct {
	app   *app.App
	gw    string
	rng   *rand.Rand
	docs  []*docRun
	lat   *latencies
	count counters
	start time.Time
	probe error
	// zombieDocs 是 zombie node 被隔離時手上的文件。
	zombieDocs map[string]bool
}

func run(seed uint64, docs int, duration time.Duration, logger *slog.Logger) error {
	dir, err := os.MkdirTemp("", "docs-sim-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	a, err := app.New(simOptions(filepath.Join(dir, "docs.db"), logger))
	if err != nil {
		return err
	}
	defer a.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a.Start(ctx)
	for range 3 {
		a.AddEditNode(listen(), "")
	}
	for range 2 {
		a.AddViewNode(listen(), "")
	}
	gw := httptest.NewServer(a.Gateway.Handler())
	defer gw.Close()

	s := &sim{app: a, gw: gw.URL, rng: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		lat: &latencies{typed: make(map[string]time.Time)}, zombieDocs: make(map[string]bool)}
	setupStart := time.Now()
	if err := s.setup(ctx, docs); err != nil {
		return err
	}
	editors, viewers := 0, 0
	for _, d := range s.docs {
		editors += len(d.agents)
		viewers += len(d.viewers)
	}
	fmt.Printf("setup     %d documents: 1 hot document with %d editors and %d viewers, %d with 3-6 editors and 0-4 viewers\n",
		docs, hotEditors, hotViewers, docs-1)
	fmt.Printf("          %d editors (WebSocket) and %d viewers (SSE) connected through the gateway in %s\n",
		editors, viewers, time.Since(setupStart).Round(time.Millisecond))
	fmt.Printf("cluster   3 edit nodes, 2 viewer nodes, lease %s, event queue %d partitions, editor limit %d per document\n\n",
		leaseTTL, a.Bus.Partitions(), hotEditors)

	if err := s.typing(ctx, duration); err != nil {
		return err
	}
	quiesceStart := time.Now()
	if err := s.quiesce(ctx); err != nil {
		return err
	}
	fmt.Printf("quiesce   every editor and viewer caught up %s after typing stopped\n\n", time.Since(quiesceStart).Round(time.Millisecond))
	s.report(ctx)
	s.verify(ctx)
	return nil
}

func listen() net.Listener {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	return ln
}

// setup 建立文件、分享給編輯者與觀看者，並讓所有人連上。
func (s *sim) setup(ctx context.Context, docs int) error {
	agentID := 0
	for i := range docs {
		nEditors, nViewers := 3+s.rng.IntN(4), s.rng.IntN(5)
		if i == 0 {
			nEditors, nViewers = hotEditors, hotViewers
		}
		owner, err := client.Login(ctx, s.gw, fmt.Sprintf("u%d-0", i))
		if err != nil {
			return err
		}
		id, err := owner.CreateDoc(ctx, fmt.Sprintf("document %d", i))
		if err != nil {
			return err
		}
		d := &docRun{id: id, hot: i == 0, inserted: make(map[string]bool), deleted: make(map[string]bool)}
		for k := range nEditors {
			api := owner
			if k > 0 {
				user := fmt.Sprintf("u%d-%d", i, k)
				if err := owner.Share(ctx, id, user, domain.RoleEditor); err != nil {
					return err
				}
				if api, err = client.Login(ctx, s.gw, user); err != nil {
					return err
				}
			}
			agentID++
			ag := &agent{id: agentID, doc: d, rng: rand.New(rand.NewPCG(s.rng.Uint64(), uint64(agentID)))}
			ag.ed = client.NewEditor(api, client.EditorConfig{DocID: id, AckLatency: &s.lat.ack,
				OnRemote: func(op ot.Op) { s.lat.observe(op, &s.lat.remote) }})
			d.agents = append(d.agents, ag)
		}
		for k := range nViewers {
			user := fmt.Sprintf("v%d-%d", i, k)
			if err := owner.Share(ctx, id, user, domain.RoleViewer); err != nil {
				return err
			}
			api, err := client.Login(ctx, s.gw, user)
			if err != nil {
				return err
			}
			d.viewers = append(d.viewers, client.NewViewer(api, id, func(op ot.Op) { s.lat.observe(op, &s.lat.viewer) }))
		}
		if i == 0 {
			if err := owner.Share(ctx, id, "probe", domain.RoleEditor); err != nil {
				return err
			}
		}
		s.docs = append(s.docs, d)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 1)
	for _, d := range s.docs {
		for _, ag := range d.agents {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := ag.ed.Start(ctx); err != nil {
					select {
					case errs <- err:
					default:
					}
				}
			}()
		}
		for _, v := range d.viewers {
			v.Start(ctx)
		}
	}
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

type fault struct {
	at   float64 // 佔打字時間的比例
	what string
	do   func(ctx context.Context)
}

func (s *sim) typing(ctx context.Context, duration time.Duration) error {
	stop := make(chan struct{})
	errs := make(chan error, 1)
	var wg sync.WaitGroup
	s.start = time.Now()
	for _, d := range s.docs {
		// 熱門文件 100 人同時在線，每人 1-3 秒一個動作；其他文件每人 0.1-0.5 秒一個動作。
		gap := func(r *rand.Rand) time.Duration {
			return 100*time.Millisecond + time.Duration(r.Int64N(int64(400*time.Millisecond)))
		}
		if d.hot {
			gap = func(r *rand.Rand) time.Duration { return time.Second + time.Duration(r.Int64N(int64(2*time.Second))) }
		}
		for _, ag := range d.agents {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := ag.run(stop, gap, s.lat, &s.count); err != nil {
					select {
					case errs <- err:
					default:
					}
				}
			}()
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.blips(stop)
	}()

	for _, f := range s.faults() {
		if wait := time.Until(s.start.Add(time.Duration(f.at * float64(duration)))); wait > 0 {
			time.Sleep(wait)
		}
		f.do(ctx)
		fmt.Printf("fault     %4.1fs  %s\n", time.Since(s.start).Seconds(), f.what)
	}
	time.Sleep(time.Until(s.start.Add(duration)))
	close(stop)
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
	}
	fmt.Printf("typing    %d inserts, %d deletes, %d cursor moves in %.1fs; %d dropped connections\n",
		s.count.inserts.Load(), s.count.deletes.Load(), s.count.moves.Load(), time.Since(s.start).Seconds(), s.count.blips.Load())
	return nil
}

func (s *sim) faults() []fault {
	return []fault{
		{0.19, "edit-3 joined: the hash ring moves part of the documents to it (graceful handoff)", func(ctx context.Context) {
			s.app.AddEditNode(listen(), "")
		}},
		{0.375, "edit-1 crashed: no handoff, its documents move once its lease expires", func(ctx context.Context) {
			s.app.EditNodes()[1].Kill()
		}},
		{0.56, "edit-2 cut off from the registry for 2.5s but still serving its editors (zombie)", func(ctx context.Context) {
			zombie := s.app.EditNodes()[2]
			for _, id := range zombie.Docs() {
				s.zombieDocs[id] = true
			}
			zombie.Pause(pauseFor)
			// 租約過期後，讓 zombie 的一部分編輯者斷線重連：他們被 gateway 導到新的 owner，
			// 新 owner claim 文件，留在 zombie 上的編輯者之後的寫入就會被 fence。
			time.AfterFunc(leaseTTL+400*time.Millisecond, func() {
				for _, d := range s.docs {
					if !s.zombieDocs[d.id] {
						continue
					}
					for _, ag := range d.agents {
						if rand.IntN(10) < 6 {
							ag.ed.DropConnection()
						}
					}
				}
			})
		}},
		{0.75, "view-0 crashed: its viewers reconnect with Last-Event-ID", func(ctx context.Context) {
			s.app.ViewNodes()[0].Kill()
		}},
		{0.81, "view-2 joined", func(ctx context.Context) {
			s.app.AddViewNode(listen(), "")
		}},
		{0.875, "a 101st editor tried to join the hot document", func(ctx context.Context) {
			s.probeHotDoc(ctx)
		}},
	}
}

// probeHotDoc 等熱門文件的 100 位編輯者都在線，再讓第 101 位加入。
func (s *sim) probeHotDoc(ctx context.Context) {
	hot := s.docs[0]
	waitFor(5*time.Second, func() bool {
		for _, ag := range hot.agents {
			online := false
			ag.ed.Read(func(d *client.Doc) { online = d.Online() })
			if !online {
				return false
			}
		}
		return true
	})
	api, err := client.Login(ctx, s.gw, "probe")
	if err != nil {
		s.probe = err
		return
	}
	ed := client.NewEditor(api, client.EditorConfig{DocID: hot.id})
	s.probe = ed.Start(ctx)
	if s.probe == nil {
		ed.Close()
		s.probe = errors.New("the 101st editor was admitted")
	}
}

// blips 模擬不穩定的網路：隨機讓編輯者與觀看者斷線（熱門文件的編輯者除外，避免干擾人數上限的檢查）。
func (s *sim) blips(stop <-chan struct{}) {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		for _, d := range s.docs {
			for _, ag := range d.agents {
				if !d.hot && rand.IntN(1000) < 15 {
					ag.ed.DropConnection()
					s.count.blips.Add(1)
				}
			}
			for _, v := range d.viewers {
				if rand.IntN(1000) < 15 {
					v.DropConnection()
					s.count.blips.Add(1)
				}
			}
		}
	}
}
