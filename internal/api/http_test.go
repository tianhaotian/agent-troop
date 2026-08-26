package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"agenttroop/internal/clock"
	"agenttroop/internal/core"
	"agenttroop/internal/store"
	"agenttroop/internal/store/memory"
)

func testHandler() http.Handler {
	svc := core.New(memory.New(), clock.RealClock{}, core.DefaultConfig())
	return New(svc).Handler()
}

func TestConsoleRootRoute(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	testHandler().ServeHTTP(w, r)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "Agent Troop") {
		t.Fatalf("root console: status=%d body=%q", w.Code, w.Body.String())
	}
}

func TestReadiness(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		w := httptest.NewRecorder()
		testHandler().ServeHTTP(w, r)
		if w.Code != http.StatusOK {
			t.Fatalf("ready store: status=%d body=%q", w.Code, w.Body.String())
		}
	})

	t.Run("store unavailable", func(t *testing.T) {
		st := &unreadyStore{Store: memory.New()}
		svc := core.New(st, clock.RealClock{}, core.DefaultConfig())
		r := httptest.NewRequest(http.MethodGet, "/readyz", nil)
		w := httptest.NewRecorder()
		New(svc).Handler().ServeHTTP(w, r)
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("unready store: status=%d body=%q", w.Code, w.Body.String())
		}
	})
}

type unreadyStore struct{ store.Store }

func (s *unreadyStore) Ping(context.Context) error { return errors.New("database unavailable") }

func TestBoardBodyLimitRejectsInsteadOfTruncating(t *testing.T) {
	r := httptest.NewRequest(http.MethodPut, "/v1/missions/msn_x/board/shared/large",
		bytes.NewReader(make([]byte, (1<<20)+1)))
	w := httptest.NewRecorder()
	testHandler().ServeHTTP(w, r)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized board body: status=%d body=%q", w.Code, w.Body.String())
	}
}

func TestLeadRoutesValidateRequests(t *testing.T) {
	t.Run("inbox status", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/v1/subtasks/sub_x/lead/inbox?status=bad", nil)
		w := httptest.NewRecorder()
		testHandler().ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
		}
	})
	t.Run("heartbeat requires snapshot version", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/v1/subtasks/sub_x/lead/heartbeat",
			strings.NewReader(`{"agent_id":"agt","fencing_token":1,"snapshot":{}}`))
		w := httptest.NewRecorder()
		testHandler().ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
		}
	})
	t.Run("invalid ingest mode", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/v1/subtasks/sub_x/lead/inbox/item_x/ingest",
			strings.NewReader(`{"agent_id":"agt","fencing_token":1,"expected_version":0,"mode":"raw"}`))
		w := httptest.NewRecorder()
		testHandler().ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
		}
	})
}

func TestBudgetRoutes(t *testing.T) {
	h := testHandler()

	t.Run("negative mission budget", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodPost, "/v1/missions", strings.NewReader(
			`{"owner":"u1","goal":"g","budget_tokens":-1,"tasks":[{"name":"a","kind":"agent"}]}`))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
		}
	})

	r := httptest.NewRequest(http.MethodPost, "/v1/missions", strings.NewReader(
		`{"owner":"u1","goal":"g","budget_tokens":100,"tasks":[{"name":"a","kind":"agent"}]}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%q", w.Code, w.Body.String())
	}
	var created map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	id := created["id"].(string)
	r = httptest.NewRequest(http.MethodGet, "/v1/missions/"+id+"/budget", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("budget status=%d body=%q", w.Code, w.Body.String())
	}
	var budget struct {
		Account store.BudgetAccount `json:"account"`
		Holds   []store.BudgetHold  `json:"holds"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &budget); err != nil {
		t.Fatal(err)
	}
	if !budget.Account.Metered || budget.Account.Total != 100 || budget.Account.Available != 100 || len(budget.Holds) != 0 {
		t.Fatalf("budget response=%+v", budget)
	}

	r = httptest.NewRequest(http.MethodPost, "/v1/subtasks/sub_missing/complete", strings.NewReader(
		`{"agent_id":"agt","fencing_token":1,"version":1,"idempotency_key":"x","usage_tokens":-1}`))
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("negative usage status=%d body=%q", w.Code, w.Body.String())
	}
}

func TestContextPermissionRoutes(t *testing.T) {
	h := testHandler()
	r := httptest.NewRequest(http.MethodPost, "/v1/missions", strings.NewReader(
		`{"owner":"u1","goal":"bad","tasks":[{"name":"a","kind":"agent","grants":{"classification":"secret"}}]}`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid grants status=%d body=%q", w.Code, w.Body.String())
	}
	r = httptest.NewRequest(http.MethodGet, "/v1/leases/les_missing/context", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("missing context status=%d body=%q", w.Code, w.Body.String())
	}
}
