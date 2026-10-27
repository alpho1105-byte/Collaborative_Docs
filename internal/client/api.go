// Package client 是協作文件的 client：gateway 的 HTTP API、編輯者（WebSocket + OT）與觀看者（SSE）。
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"collaborative-docs/internal/domain"
)

type HTTPError struct {
	Status  int
	Message string
}

func (e *HTTPError) Error() string { return fmt.Sprintf("HTTP %d: %s", e.Status, e.Message) }

// API 呼叫 gateway。
type API struct {
	Base  string
	Token string
	http  *http.Client
}

func NewAPI(base, token string) *API {
	return &API{Base: base, Token: token, http: &http.Client{Timeout: 10 * time.Second}}
}

// Login 用 /dev/token 取得 token（只在開發模式提供）。
func Login(ctx context.Context, base, userID string) (*API, error) {
	a := NewAPI(base, "")
	var out struct {
		Token string `json:"token"`
	}
	if err := a.do(ctx, http.MethodPost, "/dev/token", map[string]string{"user_id": userID}, &out); err != nil {
		return nil, err
	}
	a.Token = out.Token
	return a, nil
}

func (a *API) CreateDoc(ctx context.Context, title string) (string, error) {
	var out struct {
		DocID string `json:"doc_id"`
	}
	err := a.do(ctx, http.MethodPost, "/v1/docs", map[string]string{"title": title}, &out)
	return out.DocID, err
}

func (a *API) Share(ctx context.Context, docID, userID string, role domain.Role) error {
	return a.do(ctx, http.MethodPost, "/v1/docs/"+docID+"/members", map[string]any{"user_id": userID, "role": role}, nil)
}

// EditURL 問 gateway 文件的 owner 在哪一台 node。
func (a *API) EditURL(ctx context.Context, docID string) (string, error) {
	return a.url(ctx, "/v1/docs/"+docID+"/edit")
}

func (a *API) ViewURL(ctx context.Context, docID string) (string, error) {
	return a.url(ctx, "/v1/docs/"+docID+"/view")
}

func (a *API) url(ctx context.Context, path string) (string, error) {
	var out struct {
		URL string `json:"url"`
	}
	err := a.do(ctx, http.MethodGet, path, nil, &out)
	return out.URL, err
}

func (a *API) do(ctx context.Context, method, path string, body, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.Base+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if a.Token != "" {
		req.Header.Set("Authorization", "Bearer "+a.Token)
	}
	resp, err := a.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		var e struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&e)
		return &HTTPError{Status: resp.StatusCode, Message: e.Error}
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
