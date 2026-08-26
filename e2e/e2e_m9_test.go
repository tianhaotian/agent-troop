//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"agenttroop/internal/api"
	"agenttroop/internal/clock"
	"agenttroop/internal/core"
	"agenttroop/internal/store/memory"
)

func TestE2EM9QualityReputationMeteringAndMetrics(t *testing.T) {
	svc := core.New(memory.New(), clock.RealClock{}, core.DefaultConfig())
	srv := httptest.NewServer(api.New(svc).Handler())
	defer srv.Close()
	base = srv.URL
	fakeAgent(t, "agt_m9_producer", "write")
	fakeAgent(t, "agt_m9_judge", "verify")
	created := do(t, "POST", "/v1/missions", map[string]any{
		"owner": "e2e", "goal": "M9 quality",
		"tasks": []map[string]any{{"name": "draft", "kind": "agent", "required_skills": []string{"write"}}},
	}, 201)
	missionID := created["id"].(string)
	if _, err := svc.ScheduleOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	offers := do(t, "GET", "/v1/agents/agt_m9_producer/offers", nil, 200)["offers"].([]any)
	offer := offers[0].(map[string]any)
	sub := offer["subtask"].(map[string]any)
	subID := sub["id"].(string)
	leaseID := offer["lease_id"].(string)
	token := int64(offer["fencing_token"].(float64))
	accepted := do(t, "POST", "/v1/leases/"+leaseID+"/accept", map[string]any{
		"agent_id": "agt_m9_producer", "fencing_token": token,
		"subtask_version": int64(sub["version"].(float64)),
	}, 200)
	started := do(t, "POST", "/v1/subtasks/"+subID+"/start", map[string]any{
		"agent_id": "agt_m9_producer", "fencing_token": token,
		"version": int64(accepted["version"].(float64)),
	}, 200)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/artifacts", strings.NewReader(`{"draft":"ok"}`))
	req.Header.Set("X-Mission-ID", missionID)
	req.Header.Set("X-Produced-By", subID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	artifactRaw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("artifact=%d body=%s", resp.StatusCode, artifactRaw)
	}
	var artifact map[string]any
	_ = json.Unmarshal(artifactRaw, &artifact)
	artifactID := artifact["id"].(string)
	do(t, "POST", "/v1/subtasks/"+subID+"/complete", map[string]any{
		"agent_id": "agt_m9_producer", "fencing_token": token,
		"version": int64(started["version"].(float64)), "idempotency_key": "m9-e2e",
		"result_ref": "artifact://" + artifactID, "usage_tokens": 42,
	}, 200)
	do(t, "POST", "/v1/artifacts/"+artifactID+"/verify", map[string]any{
		"verifier_agent_id": "agt_m9_judge", "score": 0.9, "confidence": 0.9,
		"verdict": "accepted", "rubric": "rubric://e2e/v1",
	}, 201)
	reputation := do(t, "GET", "/v1/agents/agt_m9_producer/reputation", nil, 200)
	if len(reputation["skills"].([]any)) != 1 {
		t.Fatalf("reputation=%v", reputation)
	}
	usage := do(t, "GET", "/v1/missions/"+missionID+"/usage", nil, 200)
	resources := usage["quantity_by_resource"].(map[string]any)
	for _, name := range []string{"artifact.byte", "lease.wall_ms", "token.reported", "verify.call"} {
		if _, ok := resources[name]; !ok {
			t.Fatalf("usage missing %s: %v", name, usage)
		}
	}
	resp, err = http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	metrics, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(metrics), "troop_http_requests_total") {
		t.Fatalf("metrics=%s", metrics)
	}
}
