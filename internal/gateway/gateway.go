// Package gateway 是 API Gateway：文件 CRUD，以及告訴 client 該連到哪一台 node。
// 它訂閱 registry 的名單，用和 node 相同的 consistent hashing 算出每份文件的 owner。
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"collaborative-docs/internal/auth"
	"collaborative-docs/internal/docs"
	"collaborative-docs/internal/domain"
	"collaborative-docs/internal/registry"
	"collaborative-docs/internal/ring"
	"collaborative-docs/internal/viewer"
)

type Deps struct {
	Signer   *auth.Signer
	Docs     *docs.Service
	Registry *registry.Registry
	// PartitionOf 回傳文件在 event queue 的分區；viewer node 依分區分工。
	PartitionOf func(docID string) int
	VNodes      int
	// Dev 開啟 POST /dev/token，方便本機測試時取得 token。
	Dev bool
	Log *slog.Logger
}

type routes struct {
	members registry.Membership
	ring    *ring.Ring
}

type Gateway struct {
	d    Deps
	edit atomic.Pointer[routes]
	view atomic.Pointer[routes]
}

func New(d Deps) *Gateway {
	return &Gateway{d: d}
}

// Run 訂閱 edit 與 view 兩類 node 的名單，直到 ctx 結束。
func (g *Gateway) Run(ctx context.Context) {
	for kind, dst := range map[string]*atomic.Pointer[routes]{registry.KindEdit: &g.edit, registry.KindView: &g.view} {
		ch := g.d.Registry.Watch(kind)
		dst.Store(g.routes(<-ch))
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case m := <-ch:
					dst.Store(g.routes(m))
				}
			}
		}()
	}
}

func (g *Gateway) routes(m registry.Membership) *routes {
	return &routes{members: m, ring: ring.New(m.IDs(), g.d.VNodes)}
}

func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/docs", g.auth(g.createDoc))
	mux.HandleFunc("GET /v1/docs/{id}", g.auth(g.getDoc))
	mux.HandleFunc("POST /v1/docs/{id}/members", g.auth(g.share))
	mux.HandleFunc("GET /v1/docs/{id}/edit", g.auth(g.editURL))
	mux.HandleFunc("GET /v1/docs/{id}/view", g.auth(g.viewURL))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	if g.d.Dev {
		mux.HandleFunc("POST /dev/token", g.token)
	}
	return mux
}

type userHandler func(w http.ResponseWriter, r *http.Request, user string)

func (g *Gateway) auth(h userHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		user, err := g.d.Signer.Verify(token)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "invalid or missing token")
			return
		}
		h(w, r, user)
	}
}

func (g *Gateway) createDoc(w http.ResponseWriter, r *http.Request, user string) {
	var req struct {
		Title string `json:"title"`
	}
	if !decode(w, r, &req) {
		return
	}
	d, err := g.d.Docs.Create(r.Context(), user, req.Title)
	if err != nil {
		g.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"doc_id": d.ID, "title": d.Title})
}

func (g *Gateway) getDoc(w http.ResponseWriter, r *http.Request, user string) {
	d, role, err := g.d.Docs.Get(r.Context(), user, r.PathValue("id"))
	if err != nil {
		g.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"doc_id": d.ID, "title": d.Title, "created_by": d.CreatedBy,
		"created_at": d.CreatedAt.UTC().Format(time.RFC3339), "role": role})
}

func (g *Gateway) share(w http.ResponseWriter, r *http.Request, user string) {
	var req struct {
		UserID string      `json:"user_id"`
		Role   domain.Role `json:"role"`
	}
	if !decode(w, r, &req) {
		return
	}
	if err := g.d.Docs.Share(r.Context(), user, r.PathValue("id"), req.UserID, req.Role); err != nil {
		g.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// editURL 回傳文件 owner 的 WebSocket 位址。
func (g *Gateway) editURL(w http.ResponseWriter, r *http.Request, user string) {
	docID := r.PathValue("id")
	role, err := g.d.Docs.Role(r.Context(), docID, user)
	if err != nil {
		g.fail(w, err)
		return
	}
	if !role.CanEdit() {
		writeError(w, http.StatusForbidden, "viewers cannot edit; use the view endpoint")
		return
	}
	g.route(w, g.edit.Load(), docID, func(addr string) string { return "ws://" + addr + "/ws/docs/" + docID })
}

// viewURL 回傳負責該文件分區的 viewer node 的 SSE 位址。
func (g *Gateway) viewURL(w http.ResponseWriter, r *http.Request, user string) {
	docID := r.PathValue("id")
	if _, err := g.d.Docs.Role(r.Context(), docID, user); err != nil {
		g.fail(w, err)
		return
	}
	g.route(w, g.view.Load(), viewer.PartitionKey(g.d.PartitionOf(docID)), func(addr string) string { return "http://" + addr + "/sse/docs/" + docID })
}

func (g *Gateway) route(w http.ResponseWriter, rt *routes, key string, url func(addr string) string) {
	var node, addr string
	ok := rt != nil
	if ok {
		node, ok = rt.ring.Owner(key)
	}
	if ok {
		addr, ok = rt.members.Addr(node)
	}
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "no node available")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"url": url(addr), "node": node})
}

func (g *Gateway) token(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID string `json:"user_id"`
	}
	if !decode(w, r, &req) || req.UserID == "" {
		writeError(w, http.StatusBadRequest, "user_id is required")
		return
	}
	tok, err := g.d.Signer.Issue(req.UserID, 24*time.Hour)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "issue token")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"token": tok})
}

func (g *Gateway) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, docs.ErrInvalid):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "document not found")
	case errors.Is(err, domain.ErrForbidden):
		writeError(w, http.StatusForbidden, "only the owner can share this document")
	default:
		g.d.Log.Error("request failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
