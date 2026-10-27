package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"collaborative-docs/internal/app"
)

func main() {
	addr := flag.String("addr", ":8098", "gateway listen address")
	db := flag.String("db", "data/docs.db", "SQLite database path")
	host := flag.String("host", "127.0.0.1", "host the document nodes listen on and advertise to clients")
	editNodes := flag.Int("edit-nodes", 3, "number of edit nodes")
	editPort := flag.Int("edit-port", 8101, "port of the first edit node")
	viewNodes := flag.Int("view-nodes", 2, "number of viewer nodes")
	viewPort := flag.Int("view-port", 8111, "port of the first viewer node")
	dev := flag.Bool("dev", true, "enable POST /dev/token")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(*addr, *db, *host, *editNodes, *editPort, *viewNodes, *viewPort, *dev, logger); err != nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func run(addr, db, host string, editNodes, editPort, viewNodes, viewPort int, dev bool, logger *slog.Logger) error {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		if !dev {
			return errors.New("JWT_SECRET is required when -dev=false")
		}
		secret = "dev-only-secret"
		logger.Warn("JWT_SECRET not set, using an insecure development secret")
	}
	opts := app.DefaultOptions(db, []byte(secret))
	opts.Dev = dev
	opts.Logger = logger
	a, err := app.New(opts)
	if err != nil {
		return err
	}
	defer a.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	a.Start(ctx)
	for i := range editNodes {
		ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, editPort+i))
		if err != nil {
			return err
		}
		a.AddEditNode(ln, "")
	}
	for i := range viewNodes {
		ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, viewPort+i))
		if err != nil {
			return err
		}
		a.AddViewNode(ln, "")
	}

	srv := &http.Server{Addr: addr, Handler: a.Gateway.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, n := range a.EditNodes() {
			n.Shutdown(shutdownCtx)
		}
		srv.Shutdown(shutdownCtx)
	}()

	logger.Info("docs gateway listening", "addr", addr, "db", db, "edit_nodes", editNodes, "view_nodes", viewNodes)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
