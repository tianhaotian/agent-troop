// http-echo：通用 HTTP Adapter 参考实现（M1 切片 S10）。
//
// 行为：注册 → 轮询 offer → accept → start → 回显输入作为结果 complete。
// 用途：端到端自测；第三方平台接入的协议兼容性基准（设计 §6.3）。
//
//	go run ./adapters/http-echo -server http://localhost:8080 -id echo1 -skills web.research
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

var (
	server = flag.String("server", "http://localhost:8080", "troopd base URL")
	id     = flag.String("id", "echo1", "agent id")
	skills = flag.String("skills", "web.research", "comma-separated skill list")
	poll   = flag.Duration("poll", 500*time.Millisecond, "offer poll interval")
)

func main() {
	flag.Parse()
	caps := []map[string]any{}
	for _, sk := range strings.Split(*skills, ",") {
		if sk = strings.TrimSpace(sk); sk != "" {
			caps = append(caps, map[string]any{"skill": sk, "level": 0.9})
		}
	}
	must(post("/v1/agents/register", map[string]any{
		"id": *id, "name": *id, "platform": "http-echo",
		"capabilities": caps, "max_concurrency": 2,
	}, nil))
	log.Printf("registered as %s with skills %s", *id, *skills)

	// 心跳循环
	go func() {
		for range time.Tick(10 * time.Second) {
			_ = post("/v1/agents/"+*id+"/heartbeat", map[string]any{}, nil)
		}
	}()

	// 轮询执行循环
	for range time.Tick(*poll) {
		var offers struct {
			Offers []struct {
				Subtask struct {
					ID      string `json:"id"`
					Version int64  `json:"version"`
					LeaseID string `json:"lease_id"`
				} `json:"subtask"`
				LeaseID      string `json:"lease_id"`
				FencingToken int64  `json:"fencing_token"`
			} `json:"offers"`
		}
		if err := get("/v1/agents/"+*id+"/offers", &offers); err != nil || len(offers.Offers) == 0 {
			continue
		}
		for _, o := range offers.Offers {
			runTask(o.Subtask.ID, o.Subtask.Version, o.LeaseID, o.FencingToken)
		}
	}
}

func runTask(subtaskID string, version int64, leaseID string, token int64) {
	log.Printf("accepting %s (lease %s)", subtaskID, leaseID)
	var accepted struct {
		Version int64 `json:"version"`
	}
	if err := post("/v1/leases/"+leaseID+"/accept", map[string]any{
		"agent_id": *id, "fencing_token": token, "subtask_version": version,
	}, &accepted); err != nil {
		log.Printf("accept %s: %v", subtaskID, err)
		return
	}
	var started struct {
		Version int64 `json:"version"`
	}
	if err := post("/v1/subtasks/"+subtaskID+"/start", map[string]any{
		"agent_id": *id, "fencing_token": token, "version": accepted.Version,
	}, &started); err != nil {
		log.Printf("start %s: %v", subtaskID, err)
		return
	}
	// 回显：结果即输入的引用（真实 Agent 在此调用 OpenClaw/Hermes 等运行时）
	if err := post("/v1/subtasks/"+subtaskID+"/complete", map[string]any{
		"agent_id": *id, "fencing_token": token, "version": started.Version,
		"idempotency_key": "echo-" + subtaskID,
		"result_ref":      "echo://" + *id + "/" + subtaskID,
	}, nil); err != nil {
		log.Printf("complete %s: %v", subtaskID, err)
		return
	}
	log.Printf("completed %s", subtaskID)
}

func get(path string, out any) error {
	resp, err := http.Get(*server + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

func post(path string, body, out any) error {
	buf, _ := json.Marshal(body)
	resp, err := http.Post(*server+path, "application/json", bytes.NewReader(buf))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, b)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
