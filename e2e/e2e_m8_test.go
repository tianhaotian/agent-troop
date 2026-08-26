//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"agenttroop/internal/api"
	"agenttroop/internal/auth"
	"agenttroop/internal/clock"
	"agenttroop/internal/core"
	"agenttroop/internal/store/memory"
)

func TestE2EM8AuthenticatedProtocolsAndSignedArtifact(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC))
	manager, err := auth.New("0123456789abcdef0123456789abcdef", clk)
	if err != nil {
		t.Fatal(err)
	}
	svc := core.New(memory.New(), clk, core.DefaultConfig())
	srv := httptest.NewServer(api.NewAuthenticated(svc, manager).Handler())
	defer srv.Close()
	human, _ := manager.Issue("owner@example", "human", nil, time.Hour)
	agent, _ := manager.Issue("runtime/agent-a", "agent", nil, time.Hour)

	call := func(method, path, token string, body any, want int) map[string]any {
		t.Helper()
		var reader io.Reader
		if body != nil {
			encoded, _ := json.Marshal(body)
			reader = bytes.NewReader(encoded)
		}
		req, _ := http.NewRequest(method, srv.URL+path, reader)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != want {
			t.Fatalf("%s %s=%d want=%d body=%s", method, path, resp.StatusCode, want, data)
		}
		var result map[string]any
		if len(data) > 0 {
			_ = json.Unmarshal(data, &result)
		}
		return result
	}

	call("POST", "/v1/agents/register", human, map[string]any{
		"id": "agt_a", "name": "a", "platform": "custom", "auth_subject": "runtime/agent-a",
	}, 200)
	call("POST", "/v1/agents/agt_a/heartbeat", agent, map[string]any{}, 200)
	call("POST", "/v1/agents/register", agent, map[string]any{
		"id": "agt_evil", "name": "evil", "platform": "custom",
	}, 403)

	a2a := call("POST", "/a2a", human, map[string]any{
		"jsonrpc": "2.0", "id": 1, "method": "SendMessage",
		"params": map[string]any{"message": map[string]any{"role": "ROLE_USER", "parts": []map[string]any{{"text": "M8 mission"}}}},
	}, 200)
	missionID := a2a["result"].(map[string]any)["task"].(map[string]any)["id"].(string)
	mcp := call("POST", "/mcp", human, map[string]any{
		"jsonrpc": "2.0", "id": 2, "method": "resources/read",
		"params": map[string]any{"uri": "troop://missions/" + missionID},
	}, 200)
	if _, ok := mcp["result"]; !ok {
		t.Fatalf("mcp=%v", mcp)
	}

	req, _ := http.NewRequest("POST", srv.URL+"/v1/artifacts", strings.NewReader("m8 artifact"))
	req.Header.Set("Authorization", "Bearer "+human)
	req.Header.Set("X-Mission-ID", missionID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var artifact map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&artifact)
	resp.Body.Close()
	artifactID := artifact["id"].(string)
	signed := call("POST", "/v1/artifacts/"+artifactID+"/signed-url", human,
		map[string]any{"expires_in": 60}, 200)
	resp, err = http.Get(srv.URL + signed["url"].(string))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(data) != "m8 artifact" {
		t.Fatalf("signed download=%d body=%s", resp.StatusCode, data)
	}
}
