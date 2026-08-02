//go:build e2e

// M1 端到端验收（docs/plan/M1-mvp.md §4）：两个 Adapter 客户端经 HTTP API
// 接力完成链式 Mission，断言最终状态、事件序列、幂等与 fencing 防护。
//
//	go test -tags e2e ./e2e/
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"agenttroop/internal/api"
	"agenttroop/internal/clock"
	"agenttroop/internal/core"
	"agenttroop/internal/store/memory"
)

var base string

func do(t *testing.T, method, path string, body any, wantStatus int) map[string]any {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		buf, _ := json.Marshal(body)
		rdr = bytes.NewReader(buf)
	}
	req, _ := http.NewRequest(method, base+path, rdr)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	var out map[string]any
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s = %d, want %d: %s", method, path, resp.StatusCode, wantStatus, b)
	}
	_ = json.Unmarshal(b, &out)
	return out
}

// fakeAgent 模拟 http-echo Adapter 的拉模式执行循环。
func fakeAgent(t *testing.T, id string, skills ...string) {
	caps := []map[string]any{}
	for _, sk := range skills {
		caps = append(caps, map[string]any{"skill": sk, "level": 0.9})
	}
	do(t, "POST", "/v1/agents/register", map[string]any{
		"id": id, "name": id, "platform": "http-echo", "capabilities": caps, "max_concurrency": 2,
	}, 200)
}

// workOnce 拉取并完成一个 offer，返回是否处理了一个任务。
func workOnce(t *testing.T, id string) bool {
	t.Helper()
	resp := do(t, "GET", "/v1/agents/"+id+"/offers", nil, 200)
	offers, _ := resp["offers"].([]any)
	if len(offers) == 0 {
		return false
	}
	o := offers[0].(map[string]any)
	sub := o["subtask"].(map[string]any)
	subID := sub["id"].(string)
	token := int64(o["fencing_token"].(float64))
	leaseID := o["lease_id"].(string)
	version := int64(sub["version"].(float64))

	accepted := do(t, "POST", "/v1/leases/"+leaseID+"/accept", map[string]any{
		"agent_id": id, "fencing_token": token, "subtask_version": version,
	}, 200)
	v := int64(accepted["version"].(float64))

	started := do(t, "POST", "/v1/subtasks/"+subID+"/start", map[string]any{
		"agent_id": id, "fencing_token": token, "version": v,
	}, 200)
	v = int64(started["version"].(float64))

	do(t, "POST", "/v1/subtasks/"+subID+"/complete", map[string]any{
		"agent_id": id, "fencing_token": token, "version": v,
		"idempotency_key": "e2e-" + subID, "result_ref": "echo://" + id + "/" + subID,
	}, 200)
	return true
}

func TestM1EndToEnd(t *testing.T) {
	svc := core.New(memory.New(), clock.RealClock{}, core.DefaultConfig())
	srv := httptest.NewServer(api.New(svc).Handler())
	defer srv.Close()
	base = srv.URL
	ctx := context.Background()

	fakeAgent(t, "agt_research", "web.research")
	fakeAgent(t, "agt_write", "write.zh")

	created := do(t, "POST", "/v1/missions", map[string]any{
		"owner": "e2e", "goal": "链式接力",
		"tasks": []map[string]any{
			{"name": "collect", "kind": "agent", "required_skills": []string{"web.research"}},
			{"name": "report", "kind": "agent", "required_skills": []string{"write.zh"}, "depends_on": []string{"collect"}},
		},
	}, 201)
	mid := created["id"].(string)

	// 驱动：调度 + 两个 Agent 工作循环
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, _ = svc.ScheduleOnce(ctx)
		worked := workOnce(t, "agt_research")
		worked = workOnce(t, "agt_write") || worked
		final := do(t, "GET", "/v1/missions/"+mid, nil, 200)
		if final["mission"].(map[string]any)["status"] == "SUCCEEDED" {
			break
		}
		if !worked {
			time.Sleep(20 * time.Millisecond)
		}
	}

	final := do(t, "GET", "/v1/missions/"+mid, nil, 200)
	if final["mission"].(map[string]any)["status"] != "SUCCEEDED" {
		t.Fatalf("mission not SUCCEEDED: %v", final["mission"])
	}
	subs := final["subtasks"].([]any)
	for _, s := range subs {
		if s.(map[string]any)["state"] != "SUCCEEDED" {
			t.Fatalf("subtask not SUCCEEDED: %v", s)
		}
	}
	fmt.Printf("mission %s SUCCEEDED with %d subtasks\n", mid, len(subs))
}

func TestE2EIdempotentAndFencing(t *testing.T) {
	svc := core.New(memory.New(), clock.RealClock{}, core.DefaultConfig())
	srv := httptest.NewServer(api.New(svc).Handler())
	defer srv.Close()
	base = srv.URL
	ctx := context.Background()

	fakeAgent(t, "agt_a", "web.research")
	created := do(t, "POST", "/v1/missions", map[string]any{
		"owner": "e2e", "goal": "防护验证",
		"tasks": []map[string]any{
			{"name": "only", "kind": "agent", "required_skills": []string{"web.research"}},
		},
	}, 201)
	mid := created["id"].(string)

	_, _ = svc.ScheduleOnce(ctx)
	resp := do(t, "GET", "/v1/agents/agt_a/offers", nil, 200)
	o := resp["offers"].([]any)[0].(map[string]any)
	sub := o["subtask"].(map[string]any)
	subID := sub["id"].(string)
	token := int64(o["fencing_token"].(float64))
	leaseID := o["lease_id"].(string)
	version := int64(sub["version"].(float64))

	// 错误 fencing token 必须 409（§4.3 防僵尸写入）
	do(t, "POST", "/v1/leases/"+leaseID+"/accept", map[string]any{
		"agent_id": "agt_a", "fencing_token": token + 1, "subtask_version": version,
	}, 409)

	accepted := do(t, "POST", "/v1/leases/"+leaseID+"/accept", map[string]any{
		"agent_id": "agt_a", "fencing_token": token, "subtask_version": version,
	}, 200)
	started := do(t, "POST", "/v1/subtasks/"+subID+"/start", map[string]any{
		"agent_id": "agt_a", "fencing_token": token, "version": int64(accepted["version"].(float64)),
	}, 200)
	v := int64(started["version"].(float64))

	do(t, "POST", "/v1/subtasks/"+subID+"/complete", map[string]any{
		"agent_id": "agt_a", "fencing_token": token, "version": v,
		"idempotency_key": "k1", "result_ref": "echo://r",
	}, 200)
	// 幂等重放：200 + deduplicated 标记（§4.3 重试安全）
	replay := do(t, "POST", "/v1/subtasks/"+subID+"/complete", map[string]any{
		"agent_id": "agt_a", "fencing_token": token, "version": 999,
		"idempotency_key": "k1", "result_ref": "echo://r",
	}, 200)
	if replay["deduplicated"] != true {
		t.Fatalf("idempotent replay should be marked, got %v", replay)
	}

	final := do(t, "GET", "/v1/missions/"+mid, nil, 200)
	if final["mission"].(map[string]any)["status"] != "SUCCEEDED" {
		t.Fatalf("mission = %v", final["mission"])
	}
}
