// managed-http bridges Agent Troop's lease protocol to a simple external runtime HTTP API.
// It can front OpenClaw, Hermes or a custom runtime without leaking runtime semantics into
// the control plane.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	server   = flag.String("server", "http://localhost:8080", "Agent Troop base URL")
	id       = flag.String("id", "managed1", "registered agent id")
	platform = flag.String("platform", "custom", "runtime platform: openclaw, hermes or custom")
	runtime  = flag.String("runtime", "http://localhost:9090", "external runtime base URL")
	runPath  = flag.String("run-path", "/run", "runtime execution path")
	profile  = flag.String("profile", "auto", "protocol profile: auto, generic, hermes or openclaw")
	model    = flag.String("model", "", "runtime model (RUNTIME_MODEL fallback)")
	skills   = flag.String("skills", "general", "comma-separated skills")
	poll     = flag.Duration("poll", 500*time.Millisecond, "offer poll interval")
)

var troopClient = &http.Client{Timeout: 30 * time.Second}
var runtimeClient = &http.Client{Timeout: 30 * time.Minute}

type offer struct {
	Subtask struct {
		ID      string `json:"id"`
		Version int64  `json:"version"`
		Attempt int    `json:"attempt"`
	} `json:"subtask"`
	LeaseID        string         `json:"lease_id"`
	FencingToken   int64          `json:"fencing_token"`
	ContextPackage map[string]any `json:"context_package"`
}

func main() {
	flag.Parse()
	if err := register(); err != nil {
		var statusErr *statusError
		if !errors.As(err, &statusErr) || statusErr.Code != http.StatusForbidden {
			log.Fatalf("register: %v", err)
		}
		log.Printf("registration requires a privileged token; continuing with pre-provisioned agent %s", *id)
	}
	log.Printf("managed adapter %s (%s) -> %s%s", *id, *platform, *runtime, *runPath)
	ticker := time.NewTicker(*poll)
	defer ticker.Stop()
	for range ticker.C {
		var response struct {
			Offers []offer `json:"offers"`
		}
		if err := troopRequest(context.Background(), http.MethodGet, "/v1/agents/"+*id+"/offers", nil, &response); err != nil {
			continue
		}
		for _, current := range response.Offers {
			run(current)
		}
	}
}

func register() error {
	capabilities := []map[string]any{}
	for _, skill := range strings.Split(*skills, ",") {
		if skill = strings.TrimSpace(skill); skill != "" {
			capabilities = append(capabilities, map[string]any{"skill": skill, "level": 0.9})
		}
	}
	return troopRequest(context.Background(), http.MethodPost, "/v1/agents/register", map[string]any{
		"id": *id, "name": *id, "platform": *platform, "capabilities": capabilities,
		"max_concurrency": 1, "auth_subject": envOr("TROOP_AUTH_SUBJECT", *id),
	}, nil)
}

func run(current offer) {
	ctx := context.Background()
	var accepted struct {
		Version int64 `json:"version"`
	}
	if err := troopRequest(ctx, http.MethodPost, "/v1/leases/"+current.LeaseID+"/accept", map[string]any{
		"agent_id": *id, "fencing_token": current.FencingToken, "subtask_version": current.Subtask.Version,
	}, &accepted); err != nil {
		log.Printf("accept %s: %v", current.Subtask.ID, err)
		return
	}
	var started struct {
		Version int64 `json:"version"`
	}
	if err := troopRequest(ctx, http.MethodPost, "/v1/subtasks/"+current.Subtask.ID+"/start", map[string]any{
		"agent_id": *id, "fencing_token": current.FencingToken, "version": accepted.Version,
	}, &started); err != nil {
		log.Printf("start %s: %v", current.Subtask.ID, err)
		return
	}

	stopProgress := make(chan struct{})
	go progressLoop(stopProgress, current)
	result, err := invokeRuntime(ctx, current)
	close(stopProgress)
	if err != nil {
		_ = troopRequest(ctx, http.MethodPost, "/v1/subtasks/"+current.Subtask.ID+"/fail", map[string]any{
			"agent_id": *id, "fencing_token": current.FencingToken, "version": started.Version, "reason": err.Error(),
		}, nil)
		log.Printf("runtime %s: %v", current.Subtask.ID, err)
		return
	}
	if result.ResultRef == "" {
		result.ResultRef = "runtime://" + *platform + "/" + current.Subtask.ID
	}
	if err := troopRequest(ctx, http.MethodPost, "/v1/subtasks/"+current.Subtask.ID+"/complete", map[string]any{
		"agent_id": *id, "fencing_token": current.FencingToken, "version": started.Version,
		"idempotency_key": fmt.Sprintf("managed-%s-%d", current.Subtask.ID, current.Subtask.Attempt),
		"result_ref":      result.ResultRef, "usage_tokens": result.UsageTokens,
	}, nil); err != nil {
		log.Printf("complete %s: %v", current.Subtask.ID, err)
		return
	}
	log.Printf("completed %s", current.Subtask.ID)
}

func progressLoop(stop <-chan struct{}, current offer) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			_ = troopRequest(context.Background(), http.MethodPost, "/v1/subtasks/"+current.Subtask.ID+"/progress", map[string]any{
				"agent_id": *id, "lease_id": current.LeaseID, "fencing_token": current.FencingToken,
			}, nil)
		}
	}
}

type runtimeResult struct {
	ResultRef   string `json:"result_ref"`
	UsageTokens int64  `json:"usage_tokens,omitempty"`
}

func invokeRuntime(ctx context.Context, current offer) (runtimeResult, error) {
	selected := *profile
	if selected == "auto" {
		selected = *platform
	}
	path := *runPath
	payload := any(map[string]any{"task": current.Subtask, "context_package": current.ContextPackage})
	if selected == "hermes" || selected == "openclaw" {
		path = "/v1/responses"
		selectedModel := *model
		if selectedModel == "" {
			selectedModel = envOr("RUNTIME_MODEL", "default")
		}
		contextJSON, _ := json.Marshal(current.ContextPackage)
		payload = map[string]any{"model": selectedModel, "input": string(contextJSON), "metadata": map[string]string{
			"troop_subtask_id": current.Subtask.ID, "troop_lease_id": current.LeaseID,
		}}
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(*runtime, "/")+path, bytes.NewReader(body))
	if err != nil {
		return runtimeResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token := os.Getenv("RUNTIME_TOKEN"); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := runtimeClient.Do(req)
	if err != nil {
		return runtimeResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return runtimeResult{}, fmt.Errorf("runtime status %d: %s", resp.StatusCode, data)
	}
	var raw struct {
		ID          string `json:"id"`
		ResultRef   string `json:"result_ref"`
		UsageTokens int64  `json:"usage_tokens"`
		Usage       struct {
			InputTokens  int64 `json:"input_tokens"`
			OutputTokens int64 `json:"output_tokens"`
			TotalTokens  int64 `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&raw); err != nil {
		return runtimeResult{}, err
	}
	result := runtimeResult{ResultRef: raw.ResultRef, UsageTokens: raw.UsageTokens}
	if selected == "hermes" || selected == "openclaw" {
		result.ResultRef = "runtime://" + selected + "/responses/" + raw.ID
		result.UsageTokens = raw.Usage.TotalTokens
		if result.UsageTokens == 0 {
			result.UsageTokens = raw.Usage.InputTokens + raw.Usage.OutputTokens
		}
	}
	if result.UsageTokens < 0 {
		return runtimeResult{}, errors.New("runtime returned negative usage_tokens")
	}
	return result, nil
}

type statusError struct {
	Code int
	Body string
}

func (e *statusError) Error() string { return fmt.Sprintf("status %d: %s", e.Code, e.Body) }

func troopRequest(ctx context.Context, method, path string, body, out any) error {
	var reader io.Reader
	if body != nil {
		encoded, _ := json.Marshal(body)
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(*server, "/")+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token := os.Getenv("TROOP_TOKEN"); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := troopClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return &statusError{Code: resp.StatusCode, Body: string(data)}
	}
	if out != nil {
		return json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(out)
	}
	return nil
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
