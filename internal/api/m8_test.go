package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"agenttroop/internal/auth"
	"agenttroop/internal/clock"
	"agenttroop/internal/core"
	"agenttroop/internal/store/memory"
)

const testAuthSecret = "0123456789abcdef0123456789abcdef"

func authenticatedHandler(t *testing.T) (http.Handler, *auth.Manager) {
	t.Helper()
	clk := clock.NewFake(time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC))
	manager, err := auth.New(testAuthSecret, clk)
	if err != nil {
		t.Fatal(err)
	}
	svc := core.New(memory.New(), clk, core.DefaultConfig())
	return NewAuthenticated(svc, manager).Handler(), manager
}

func bearerRequest(method, target, body, token string) *http.Request {
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return r
}

func TestM8AuthenticationAndAgentBinding(t *testing.T) {
	h, manager := authenticatedHandler(t)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("public health=%d", w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/missions", strings.NewReader(`{}`)))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("anonymous=%d body=%s", w.Code, w.Body.String())
	}

	human, _ := manager.Issue("human@example", "human", nil, time.Hour)
	register := func(id, subject string) {
		body := `{"id":"` + id + `","name":"` + id + `","platform":"custom","auth_subject":"` + subject + `"}`
		w := httptest.NewRecorder()
		h.ServeHTTP(w, bearerRequest(http.MethodPost, "/v1/agents/register", body, human))
		if w.Code != http.StatusOK {
			t.Fatalf("register %s=%d body=%s", id, w.Code, w.Body.String())
		}
	}
	register("agt_a", "runtime-a")
	register("agt_b", "runtime-b")
	agentToken, _ := manager.Issue("runtime-a", "agent", nil, time.Hour)

	w = httptest.NewRecorder()
	h.ServeHTTP(w, bearerRequest(http.MethodPost, "/v1/agents/agt_a/heartbeat", `{}`, agentToken))
	if w.Code != http.StatusOK {
		t.Fatalf("own heartbeat=%d body=%s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, bearerRequest(http.MethodPost, "/v1/agents/agt_b/heartbeat", `{}`, agentToken))
	if w.Code != http.StatusForbidden {
		t.Fatalf("cross-agent heartbeat=%d body=%s", w.Code, w.Body.String())
	}
}

func TestM8BootstrapTokenAndSignedArtifactURL(t *testing.T) {
	h, manager := authenticatedHandler(t)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/auth/tokens",
		strings.NewReader(`{"subject":"ops","kind":"service","ttl_seconds":60}`))
	r.Header.Set("X-Troop-Bootstrap-Token", testAuthSecret)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusCreated {
		t.Fatalf("issue=%d body=%s", w.Code, w.Body.String())
	}
	var issued struct {
		AccessToken string `json:"access_token"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &issued)
	if identity, err := manager.Verify(issued.AccessToken); err != nil || identity.Subject != "ops" {
		t.Fatalf("issued identity=%+v err=%v", identity, err)
	}

	human, _ := manager.Issue("owner", "human", nil, time.Hour)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, bearerRequest(http.MethodPost, "/v1/missions",
		`{"owner":"owner","goal":"g","tasks":[{"name":"a","kind":"agent"}]}`, human))
	var created map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &created)
	mid, _ := created["id"].(string)
	if w.Code != http.StatusCreated || mid == "" {
		t.Fatalf("create=%d body=%s", w.Code, w.Body.String())
	}

	r = bearerRequest(http.MethodPost, "/v1/artifacts", "signed content", human)
	r.Header.Set("X-Mission-ID", mid)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var artifact map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &artifact)
	aid, _ := artifact["id"].(string)
	if w.Code != http.StatusCreated || aid == "" {
		t.Fatalf("artifact=%d body=%s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	h.ServeHTTP(w, bearerRequest(http.MethodPost, "/v1/artifacts/"+aid+"/signed-url", `{}`, human))
	var signed map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &signed)
	download, _ := signed["url"].(string)
	if w.Code != http.StatusOK || download == "" {
		t.Fatalf("sign=%d body=%s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, download, nil))
	if w.Code != http.StatusOK || w.Body.String() != "signed content" {
		t.Fatalf("download=%d body=%s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, strings.Replace(download, "subject=owner", "subject=attacker", 1), nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("tampered=%d body=%s", w.Code, w.Body.String())
	}
}

func TestM8A2AAndMCP(t *testing.T) {
	h := testHandler()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/.well-known/agent-card.json", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "mission-orchestration") {
		t.Fatalf("card=%d body=%s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/a2a", strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"method":"SendMessage","params":{"message":{"messageId":"m1","role":"ROLE_USER","parts":[{"text":"research batteries"}]}}}`)))
	if w.Code != http.StatusOK {
		t.Fatalf("a2a=%d body=%s", w.Code, w.Body.String())
	}
	var a2a struct {
		Result struct {
			Task struct {
				ID string `json:"id"`
			} `json:"task"`
		} `json:"result"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &a2a)
	if a2a.Result.Task.ID == "" {
		t.Fatalf("a2a result=%s", w.Body.String())
	}

	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(
		`{"jsonrpc":"2.0","id":"discover","method":"server/discover","params":{}}`)))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "2026-07-28") || !strings.Contains(w.Body.String(), "resultType") {
		t.Fatalf("mcp discover=%d body=%s", w.Code, w.Body.String())
	}

	r := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(
		`{"jsonrpc":"2.0","id":"tools","method":"tools/list","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}}`))
	r.Header.Set("Mcp-Method", "tools/list")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"ttlMs"`) || !strings.Contains(w.Body.String(), `"resultType":"complete"`) {
		t.Fatalf("current mcp=%d body=%s", w.Code, w.Body.String())
	}

	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"troop.get_mission","arguments":{"id":"`+a2a.Result.Task.ID+`"}}}`)))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), a2a.Result.Task.ID) || !strings.Contains(w.Body.String(), "structuredContent") {
		t.Fatalf("mcp=%d body=%s", w.Code, w.Body.String())
	}
}
