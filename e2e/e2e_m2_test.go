//go:build e2e

// M2 端到端验收（docs/plan/M2-hitl.md §4）：
// 生成(agent) → 审批(human) → 发布(agent) 流水线，含一次 reject 后人工改批；
// 黑板读写与 Artifact 上传下载。
package e2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
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

func TestE2EApprovalPipeline(t *testing.T) {
	svc := core.New(memory.New(), clock.RealClock{}, core.DefaultConfig())
	srv := httptest.NewServer(api.New(svc).Handler())
	defer srv.Close()
	base = srv.URL
	ctx := context.Background()

	fakeAgent(t, "agt_a", "web.research")

	created := do(t, "POST", "/v1/missions", map[string]any{
		"owner": "e2e", "goal": "生成→审批→发布",
		"tasks": []map[string]any{
			{"name": "draft", "kind": "agent", "required_skills": []string{"web.research"}},
			{"name": "gate", "kind": "human_approval", "depends_on": []string{"draft"},
				"question": "发布该草稿？"},
			{"name": "publish", "kind": "agent", "required_skills": []string{"web.research"},
				"depends_on": []string{"gate"}},
		},
	}, 201)
	mid := created["id"].(string)

	// 完成 draft
	pump := func() {
		_, _ = svc.ScheduleOnce(ctx)
		_, _ = svc.OpenHumanDecisions(ctx)
		workOnce(t, "agt_a")
	}
	pump()
	pump()

	// 审批工单应已生成，publish 未开始
	ds := do(t, "GET", "/v1/decisions?mission_id="+mid+"&status=pending", nil, 200)["decisions"].([]any)
	if len(ds) != 1 {
		t.Fatalf("pending decisions = %d, want 1", len(ds))
	}
	d := ds[0].(map[string]any)
	if d["question"] != "发布该草稿？" {
		t.Fatalf("question = %v", d["question"])
	}

	// 第一次：reject → Mission FAILED
	do(t, "POST", "/v1/decisions/"+d["id"].(string)+"/resolve", map[string]any{
		"choice": "reject", "rationale": "数据存疑", "decider_id": "lead",
	}, 200)
	final := do(t, "GET", "/v1/missions/"+mid, nil, 200)
	if final["mission"].(map[string]any)["status"] != "FAILED" {
		t.Fatalf("after reject mission = %v", final["mission"])
	}

	// 第二个 Mission 走 approve 路径（ reject 流程已验证，重提交新单）
	created2 := do(t, "POST", "/v1/missions", map[string]any{
		"owner": "e2e", "goal": "重提交",
		"tasks": []map[string]any{
			{"name": "draft", "kind": "agent", "required_skills": []string{"web.research"}},
			{"name": "gate", "kind": "human_approval", "depends_on": []string{"draft"}},
			{"name": "publish", "kind": "agent", "required_skills": []string{"web.research"},
				"depends_on": []string{"gate"}},
		},
	}, 201)
	mid2 := created2["id"].(string)
	pump()
	pump()
	ds = do(t, "GET", "/v1/decisions?mission_id="+mid2+"&status=pending", nil, 200)["decisions"].([]any)
	d = ds[0].(map[string]any)
	do(t, "POST", "/v1/decisions/"+d["id"].(string)+"/resolve", map[string]any{
		"choice": "approve", "decider_id": "lead",
	}, 200)
	// publish 应已被调度执行
	pump()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		pump()
		final = do(t, "GET", "/v1/missions/"+mid2, nil, 200)
		if final["mission"].(map[string]any)["status"] == "SUCCEEDED" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if final["mission"].(map[string]any)["status"] != "SUCCEEDED" {
		t.Fatalf("after approve mission = %v", final["mission"])
	}
	// 审计：决策留痕可读
	all := do(t, "GET", "/v1/decisions?mission_id="+mid2, nil, 200)["decisions"].([]any)
	if len(all) != 1 || all[0].(map[string]any)["status"] != "resolved" {
		t.Fatalf("decision audit: %v", all)
	}
	fmt.Printf("approval pipeline: reject→FAILED, approve→SUCCEEDED (audit ok)\n")
}

func TestE2EBoardAndArtifact(t *testing.T) {
	svc := core.New(memory.New(), clock.RealClock{}, core.DefaultConfig())
	srv := httptest.NewServer(api.New(svc).Handler())
	defer srv.Close()
	base = srv.URL

	created := do(t, "POST", "/v1/missions", map[string]any{
		"owner": "e2e", "goal": "board/artifact",
		"tasks": []map[string]any{{"name": "a", "kind": "agent"}},
	}, 201)
	mid := created["id"].(string)

	// 黑板：写（原始字节，不经 JSON）→ CAS 冲突 → 读
	putBoard := func(body string, expectedVersion string, wantStatus int) {
		t.Helper()
		req, _ := http.NewRequest("PUT", base+"/v1/missions/"+mid+"/board/shared/glossary",
			bytes.NewReader([]byte(body)))
		if expectedVersion != "" {
			req.Header.Set("X-Expected-Version", expectedVersion)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != wantStatus {
			t.Fatalf("board put = %v, want %d (err %v)", resp.StatusCode, wantStatus, err)
		}
		resp.Body.Close()
	}
	putBoard(`{"term":"储能"}`, "", 200)
	putBoard(`{"term":"stale"}`, "99", 409) // 过期版本 → 409
	got := do(t, "GET", "/v1/missions/"+mid+"/board/shared/glossary", nil, 200)
	raw, err := base64.StdEncoding.DecodeString(got["value"].(string)) // JSON 中 []byte 以 base64 编码
	if err != nil {
		t.Fatalf("decode board value: %v", err)
	}
	if string(raw) != `{"term":"储能"}` {
		t.Fatalf("board value = %s", raw)
	}

	// Artifact：上传 → 元数据 → 下载校验 sha256
	content := []byte("# 研报\n正文内容")
	req, _ := http.NewRequest("POST", base+"/v1/artifacts", bytes.NewReader(content))
	req.Header.Set("X-Mission-ID", mid)
	req.Header.Set("X-Produced-By", "sub_test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 201 {
		t.Fatalf("upload artifact: %v %v", resp.StatusCode, err)
	}
	var art map[string]any
	decodeResp(t, resp, &art)

	resp, err = http.Get(base + "/v1/artifacts/" + art["id"].(string) + "/content")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != art["sha256"] || string(data) != string(content) {
		t.Fatal("artifact content/sha256 mismatch")
	}
	fmt.Printf("board CAS + artifact roundtrip ok (art %s)\n", art["id"])
}

func decodeResp(t *testing.T, resp *http.Response, v any) {
	t.Helper()
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("decode: %v (%s)", err, b)
	}
}
