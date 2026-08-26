// Package api 北向 REST + SSE 接口（设计 §9；M1 无鉴权，M2 引入 OIDC/签名 token）。
// 路径风格：动作用 /{id}/action 表达（设计文档中的 ":action" 风格在 Go mux 不可达，等价替换）。
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"agenttroop/internal/auth"
	"agenttroop/internal/core"
	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

type Server struct {
	svc     *core.Service
	auth    *auth.Manager
	metrics *httpMetrics
}

func New(svc *core.Service) *Server { return &Server{svc: svc, metrics: newHTTPMetrics()} }

func NewAuthenticated(svc *core.Service, manager *auth.Manager) *Server {
	return &Server{svc: svc, auth: manager, metrics: newHTTPMetrics()}
}

func (s *Server) Handler() http.Handler {
	mux := newRouter()
	mux.handle("GET /healthz", s.healthz)
	mux.handle("GET /readyz", s.readyz)
	mux.handle("GET /.well-known/agent-card.json", s.agentCard)
	mux.handle("POST /a2a", s.a2aJSONRPC)
	mux.handle("POST /mcp", s.mcpJSONRPC)
	if s.auth != nil {
		mux.handle("POST /v1/auth/tokens", s.issueToken)
	}

	// 任务面
	mux.handle("POST /v1/missions", s.createMission)
	mux.handle("GET /v1/missions/{id}", s.getMission)
	mux.handle("GET /v1/missions/{id}/budget", s.getMissionBudget)
	mux.handle("GET /v1/missions/{id}/usage", s.getUsage)
	mux.handle("POST /v1/missions/{id}/cancel", s.cancelMission)
	mux.handle("GET /v1/missions/{id}/events", s.missionEventsSSE)

	// Agent 面
	mux.handle("POST /v1/agents/register", s.registerAgent)
	mux.handle("POST /v1/agents/{id}/heartbeat", s.heartbeat)
	mux.handle("GET /v1/agents/{id}/offers", s.listOffers)
	mux.handle("GET /v1/agents/{id}/reputation", s.getReputation)

	// 执行面（Agent 回调）
	mux.handle("POST /v1/leases/{id}/accept", s.acceptLease)
	mux.handle("GET /v1/leases/{id}/context", s.getLeaseContext)
	mux.handle("POST /v1/subtasks/{id}/start", s.startSubtask)
	mux.handle("POST /v1/subtasks/{id}/progress", s.progress)
	mux.handle("POST /v1/subtasks/{id}/complete", s.completeSubtask)
	mux.handle("POST /v1/subtasks/{id}/fail", s.failSubtask)
	mux.handle("POST /v1/subtasks/{id}/suspend", s.suspend) // M3-T4
	mux.handle("POST /v1/subtasks/{id}/wake", s.wake)       // M3-T4
	// Lead 恢复闭环（M7B）
	mux.handle("GET /v1/subtasks/{id}/lead/inbox", s.listLeadInbox)
	mux.handle("POST /v1/subtasks/{id}/lead/inbox/{item}/ingest", s.ingestLeadInbox)
	mux.handle("POST /v1/subtasks/{id}/lead/heartbeat", s.leadHeartbeat)
	mux.handle("GET /v1/subtasks/{id}/lead/context", s.leadContext)

	// 人工面（M2）
	mux.handle("GET /v1/decisions", s.listDecisions)
	mux.handle("POST /v1/decisions/{id}/resolve", s.resolveDecision)
	mux.handle("POST /v1/subtasks/{id}/request_decision", s.requestDecision)

	// 黑板（M2）
	mux.handle("GET /v1/missions/{id}/board/{ns}", s.boardList)
	mux.handle("GET /v1/missions/{id}/board/{ns}/{key}", s.boardGet)
	mux.handle("PUT /v1/missions/{id}/board/{ns}/{key}", s.boardPut)

	// Artifact（M2）
	mux.handle("POST /v1/artifacts", s.putArtifact)
	mux.handle("GET /v1/artifacts/{id}", s.getArtifact)
	mux.handle("GET /v1/artifacts/{id}/content", s.getArtifactContent)
	mux.handle("POST /v1/artifacts/{id}/signed-url", s.signArtifactURL)
	mux.handle("POST /v1/artifacts/{id}/verify", s.verifyArtifact)
	mux.handle("GET /v1/artifacts/{id}/quality", s.getQuality)

	// 运行观测（M9）
	mux.handle("GET /v1/observability/snapshot", s.observabilitySnapshot)
	mux.handle("GET /metrics", s.prometheusMetrics)

	// 触发准入（M4-G3）
	mux.handle("POST /v1/intents", s.submitIntent)

	// 最小 Console（S11）
	mux.handle("GET /", s.console)
	return s.observe(s.authenticate(mux))
}

// ---- helpers ----

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
	case errors.Is(err, store.ErrConflict):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "conflict: " + err.Error()})
	case errors.Is(err, store.ErrFenced):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "fenced: stale fencing token"})
	case errors.Is(err, core.ErrInvalidDAG):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	case errors.Is(err, core.ErrInvalidQuality):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	case errors.Is(err, store.ErrDuplicate):
		writeJSON(w, http.StatusConflict, map[string]string{"error": "duplicate: " + err.Error()})
	case errors.Is(err, core.ErrInvalidCondition):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()}) // M5：CEL 注册校验
	case errors.Is(err, core.ErrInvalidLeadInput):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	case errors.Is(err, core.ErrInvalidBudget), errors.Is(err, store.ErrBudgetRequired):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	case errors.Is(err, core.ErrInvalidPermission), errors.Is(err, store.ErrPermissionExceeded):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	case errors.Is(err, store.ErrBudgetExceeded):
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
	case errors.Is(err, core.ErrForbidden):
		writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()}) // M5：触发 scope 鉴权
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad json: " + err.Error()})
		return false
	}
	return true
}

func decodeLimited(w http.ResponseWriter, r *http.Request, v any, limit int64) bool {
	body, ok := readBodyLimited(w, r, limit)
	if !ok {
		return false
	}
	if err := json.Unmarshal(body, v); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad json: " + err.Error()})
		return false
	}
	return true
}

func readBodyLimited(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	body, err := io.ReadAll(r.Body)
	if err == nil {
		return body, true
	}
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
	} else {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	}
	return nil, false
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if err := s.svc.Ready(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// ---- 任务面 ----

type createMissionReq struct {
	Owner        string          `json:"owner"`
	Goal         string          `json:"goal"`
	BudgetTokens int64           `json:"budget_tokens,omitempty"`
	Tasks        []core.TaskSpec `json:"tasks"`
}

func (s *Server) createMission(w http.ResponseWriter, r *http.Request) {
	var req createMissionReq
	if !decode(w, r, &req) {
		return
	}
	if req.Owner == "" || req.Goal == "" || len(req.Tasks) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "owner/goal/tasks required"})
		return
	}
	actor := requestActor(r, store.Actor{Kind: "human", ID: req.Owner})
	m, err := s.svc.CreateMissionWithBudgetAs(r.Context(), actor, req.Owner, req.Goal, req.BudgetTokens, req.Tasks)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, m)
}

func (s *Server) getMission(w http.ResponseWriter, r *http.Request) {
	id := pv(r, "id")
	m, err := s.svc.GetMission(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	subs, err := s.svc.ListSubtasks(r.Context(), id)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"mission": m, "subtasks": subs})
}

func (s *Server) getMissionBudget(w http.ResponseWriter, r *http.Request) {
	account, holds, err := s.svc.GetMissionBudget(r.Context(), pv(r, "id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"account": account, "holds": holds})
}

func (s *Server) cancelMission(w http.ResponseWriter, r *http.Request) {
	id := pv(r, "id")
	var body struct {
		Owner string `json:"owner"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.Owner == "" {
		body.Owner = "anonymous"
	}
	actor := requestActor(r, store.Actor{Kind: "human", ID: body.Owner})
	if err := s.svc.CancelMissionAs(r.Context(), id, actor); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelled"})
}

// missionEventsSSE 事件流（Watcher 只读订阅，§8.1；M1 轮询投影）。
func (s *Server) missionEventsSSE(w http.ResponseWriter, r *http.Request) {
	id := pv(r, "id")
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "streaming unsupported"})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	var after int64
	if v := r.URL.Query().Get("after_seq"); v != "" {
		after, _ = strconv.ParseInt(v, 10, 64)
	}
	// 轮询投影（M1）；M3 事件总线外迁后改为订阅推送
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			evs, err := s.svc.ListMissionEvents(r.Context(), id, after, 200)
			if err != nil {
				return
			}
			for _, e := range evs {
				data, _ := json.Marshal(e)
				fmt.Fprintf(w, "id: %d\ndata: %s\n\n", e.Seq, data)
				after = e.Seq
			}
			flusher.Flush()
		}
	}
}

// ---- Agent 面 ----

type registerAgentReq struct {
	ID             string             `json:"id,omitempty"`
	Name           string             `json:"name"`
	Platform       string             `json:"platform"`
	Endpoint       map[string]string  `json:"endpoint,omitempty"`
	Capabilities   []store.Capability `json:"capabilities"`
	MaxConcurrency int                `json:"max_concurrency,omitempty"`
	// TriggerScopes M5-H2：触发授权（§7.4；缺省 [] 默认收紧）
	TriggerScopes []string `json:"trigger_scopes,omitempty"`
	AuthSubject   string   `json:"auth_subject,omitempty"`
}

func (s *Server) registerAgent(w http.ResponseWriter, r *http.Request) {
	var req registerAgentReq
	if !decode(w, r, &req) {
		return
	}
	if req.Name == "" || req.Platform == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name/platform required"})
		return
	}
	a := &store.Agent{
		ID: req.ID, Name: req.Name, Platform: req.Platform, Endpoint: req.Endpoint,
		Capabilities: req.Capabilities, MaxConcurrency: req.MaxConcurrency,
		TriggerScopes: req.TriggerScopes, AuthSubject: req.AuthSubject,
	}
	if err := s.svc.RegisterAgent(r.Context(), a); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	if !s.requireAgentIdentity(w, r, pv(r, "id")) {
		return
	}
	if err := s.svc.Heartbeat(r.Context(), pv(r, "id")); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type offerView struct {
	Subtask        *mission.Subtask      `json:"subtask"`
	LeaseID        string                `json:"lease_id"`
	FencingToken   int64                 `json:"fencing_token"`
	ContextPackage *store.ContextPackage `json:"context_package"`
}

func (s *Server) listOffers(w http.ResponseWriter, r *http.Request) {
	if !s.requireAgentIdentity(w, r, pv(r, "id")) {
		return
	}
	offers, err := s.svc.ListOffers(r.Context(), pv(r, "id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	views := make([]offerView, 0, len(offers))
	for _, sub := range offers {
		l, err := s.svc.GetLease(r.Context(), sub.LeaseID)
		if err != nil {
			continue
		}
		pkg, err := s.svc.GetContextPackage(r.Context(), l.ID)
		if err != nil {
			continue
		}
		views = append(views, offerView{Subtask: sub, LeaseID: l.ID,
			FencingToken: l.FencingToken, ContextPackage: pkg})
	}
	writeJSON(w, http.StatusOK, map[string]any{"offers": views})
}

func (s *Server) getLeaseContext(w http.ResponseWriter, r *http.Request) {
	if s.auth != nil {
		lease, err := s.svc.GetLease(r.Context(), pv(r, "id"))
		if err != nil {
			writeErr(w, err)
			return
		}
		if !s.requireAgentOrPrivileged(w, r, lease.AgentID) {
			return
		}
	}
	pkg, err := s.svc.GetContextPackage(r.Context(), pv(r, "id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, pkg)
}

// ---- 执行面 ----

type acceptReq struct {
	AgentID        string `json:"agent_id"`
	FencingToken   int64  `json:"fencing_token"`
	SubtaskVersion int64  `json:"subtask_version"`
}

func (s *Server) acceptLease(w http.ResponseWriter, r *http.Request) {
	var req acceptReq
	if !decode(w, r, &req) {
		return
	}
	if !s.requireAgentIdentity(w, r, req.AgentID) {
		return
	}
	sub, err := s.svc.AcceptLease(r.Context(), pv(r, "id"), req.FencingToken, req.SubtaskVersion, req.AgentID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

type fencedReq struct {
	AgentID      string `json:"agent_id"`
	FencingToken int64  `json:"fencing_token"`
	Version      int64  `json:"version"`
}

func (s *Server) startSubtask(w http.ResponseWriter, r *http.Request) {
	var req fencedReq
	if !decode(w, r, &req) {
		return
	}
	if !s.requireAgentIdentity(w, r, req.AgentID) {
		return
	}
	sub, err := s.svc.StartSubtask(r.Context(), pv(r, "id"), req.FencingToken, req.Version, req.AgentID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

type progressReq struct {
	AgentID      string          `json:"agent_id"`
	LeaseID      string          `json:"lease_id"`
	FencingToken int64           `json:"fencing_token"`
	Checkpoint   json.RawMessage `json:"checkpoint,omitempty"` // M3-T3：检查点续跑
}

func (s *Server) progress(w http.ResponseWriter, r *http.Request) {
	var req progressReq
	if !decode(w, r, &req) {
		return
	}
	if !s.requireAgentIdentity(w, r, req.AgentID) {
		return
	}
	if err := s.svc.Progress(r.Context(), pv(r, "id"), req.LeaseID, req.FencingToken, req.AgentID, req.Checkpoint); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "renewed"})
}

func (s *Server) listLeadInbox(w http.ResponseWriter, r *http.Request) {
	if !s.requireSubtaskOrPrivileged(w, r, pv(r, "id")) {
		return
	}
	status := r.URL.Query().Get("status")
	if status == "" {
		status = store.LeadInboxPending
	}
	if status != store.LeadInboxPending && status != "all" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "status must be pending or all"})
		return
	}
	items, err := s.svc.ListLeadInbox(r.Context(), pv(r, "id"), status == store.LeadInboxPending)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

type ingestLeadInboxReq struct {
	AgentID         string `json:"agent_id"`
	FencingToken    int64  `json:"fencing_token"`
	ExpectedVersion int64  `json:"expected_version"`
	Mode            string `json:"mode"`
}

func (s *Server) ingestLeadInbox(w http.ResponseWriter, r *http.Request) {
	var req ingestLeadInboxReq
	if !decode(w, r, &req) {
		return
	}
	if !s.requireAgentIdentity(w, r, req.AgentID) {
		return
	}
	item, err := s.svc.IngestLeadInbox(r.Context(), pv(r, "id"), pv(r, "item"), req.AgentID,
		req.FencingToken, req.ExpectedVersion, req.Mode)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

type leadHeartbeatReq struct {
	AgentID         string          `json:"agent_id"`
	FencingToken    int64           `json:"fencing_token"`
	ExpectedVersion *int64          `json:"expected_version"`
	Snapshot        json.RawMessage `json:"snapshot"`
}

func (s *Server) leadHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req leadHeartbeatReq
	if !decode(w, r, &req) {
		return
	}
	if !s.requireAgentIdentity(w, r, req.AgentID) {
		return
	}
	if req.ExpectedVersion == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "expected_version required"})
		return
	}
	entry, err := s.svc.SaveLeadSnapshot(r.Context(), pv(r, "id"), req.AgentID,
		req.FencingToken, *req.ExpectedVersion, req.Snapshot)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, entry)
}

func (s *Server) leadContext(w http.ResponseWriter, r *http.Request) {
	if !s.requireSubtaskOrPrivileged(w, r, pv(r, "id")) {
		return
	}
	ctx, err := s.svc.GetLeadContext(r.Context(), pv(r, "id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, ctx)
}

// suspendReq M3-T4/M4：Agent 挂起自身（Continuation，§7.3）。
type suspendReq struct {
	AgentID      string           `json:"agent_id"`
	FencingToken int64            `json:"fencing_token"`
	Version      int64            `json:"version"`
	WakeOn       mission.WakeSpec `json:"wake_on"`
	Checkpoint   json.RawMessage  `json:"checkpoint,omitempty"`
}

func (s *Server) suspend(w http.ResponseWriter, r *http.Request) {
	var req suspendReq
	if !decode(w, r, &req) {
		return
	}
	if !s.requireAgentIdentity(w, r, req.AgentID) {
		return
	}
	sub, err := s.svc.Suspend(r.Context(), pv(r, "id"), req.FencingToken, req.Version,
		req.AgentID, &req.WakeOn, req.Checkpoint)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

// wakeReq M3-T4：人工唤醒 WAITING 子任务。
type wakeReq struct {
	ActorID string `json:"actor_id"`
}

func (s *Server) wake(w http.ResponseWriter, r *http.Request) {
	var req wakeReq
	_ = json.NewDecoder(r.Body).Decode(&req) // 空体允许（默认匿名）
	if req.ActorID == "" {
		req.ActorID = "anonymous"
	}
	actor := requestActor(r, store.Actor{Kind: "human", ID: req.ActorID})
	sub, err := s.svc.WakeAs(r.Context(), pv(r, "id"), actor)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

type completeReq struct {
	AgentID        string `json:"agent_id"`
	FencingToken   int64  `json:"fencing_token"`
	Version        int64  `json:"version"`
	IdempotencyKey string `json:"idempotency_key"`
	ResultRef      string `json:"result_ref"`
	UsageTokens    int64  `json:"usage_tokens,omitempty"`
}

func (s *Server) completeSubtask(w http.ResponseWriter, r *http.Request) {
	var req completeReq
	if !decode(w, r, &req) {
		return
	}
	if !s.requireAgentIdentity(w, r, req.AgentID) {
		return
	}
	sub, err := s.svc.CompleteSubtaskWithUsage(r.Context(), pv(r, "id"),
		req.FencingToken, req.IdempotencyKey, req.ResultRef, req.UsageTokens, req.Version, req.AgentID)
	if errors.Is(err, store.ErrDuplicate) {
		// 幂等重放：返回 200 + 原状态（§4.3 重试安全）
		writeJSON(w, http.StatusOK, map[string]any{"subtask": sub, "deduplicated": true})
		return
	}
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

type failReq struct {
	AgentID      string `json:"agent_id"`
	FencingToken int64  `json:"fencing_token"`
	Version      int64  `json:"version"`
	Reason       string `json:"reason"`
}

func (s *Server) failSubtask(w http.ResponseWriter, r *http.Request) {
	var req failReq
	if !decode(w, r, &req) {
		return
	}
	if !s.requireAgentIdentity(w, r, req.AgentID) {
		return
	}
	sub, err := s.svc.FailSubtask(r.Context(), pv(r, "id"), req.FencingToken, req.Reason, req.Version, req.AgentID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

// ---- 人工面（M2） ----

func (s *Server) listDecisions(w http.ResponseWriter, r *http.Request) {
	missionID := r.URL.Query().Get("mission_id")
	pendingOnly := r.URL.Query().Get("status") == "pending"
	ds, err := s.svc.ListDecisions(r.Context(), missionID, pendingOnly)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"decisions": ds})
}

type resolveReq struct {
	Choice    string `json:"choice"`
	Rationale string `json:"rationale,omitempty"`
	DeciderID string `json:"decider_id"`
}

func (s *Server) resolveDecision(w http.ResponseWriter, r *http.Request) {
	var req resolveReq
	if !decode(w, r, &req) {
		return
	}
	if identity, ok := auth.FromContext(r.Context()); ok {
		req.DeciderID = identity.Subject
	}
	if req.Choice == "" || req.DeciderID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "choice/decider_id required"})
		return
	}
	d, err := s.svc.ResolveDecision(r.Context(), pv(r, "id"), req.Choice, req.Rationale, req.DeciderID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

type requestDecisionReq struct {
	AgentID      string   `json:"agent_id"`
	FencingToken int64    `json:"fencing_token"`
	Version      int64    `json:"version"`
	Question     string   `json:"question"`
	Options      []string `json:"options,omitempty"`
}

func (s *Server) requestDecision(w http.ResponseWriter, r *http.Request) {
	var req requestDecisionReq
	if !decode(w, r, &req) {
		return
	}
	if !s.requireAgentIdentity(w, r, req.AgentID) {
		return
	}
	if req.Question == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "question required"})
		return
	}
	d, err := s.svc.RequestDecision(r.Context(), pv(r, "id"), req.FencingToken, req.Version,
		req.AgentID, req.Question, req.Options)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, d)
}

// ---- 触发准入（M4-G3） ----

func (s *Server) submitIntent(w http.ResponseWriter, r *http.Request) {
	var in core.Intent
	if !decode(w, r, &in) {
		return
	}
	if s.auth != nil {
		identity, _ := auth.FromContext(r.Context())
		if identity.Kind == "agent" && in.Source.Kind != "agent" {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "agent token cannot claim a non-agent source"})
			return
		}
		if in.Source.Kind == "agent" && !s.requireAgentIdentity(w, r, in.Source.ID) {
			return
		}
	}
	res, err := s.svc.SubmitIntent(r.Context(), in)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// ---- 黑板（M2） ----

func (s *Server) boardList(w http.ResponseWriter, r *http.Request) {
	entries, err := s.svc.BoardList(r.Context(), pv(r, "id"), pv(r, "ns"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}

func (s *Server) boardGet(w http.ResponseWriter, r *http.Request) {
	e, err := s.svc.BoardGet(r.Context(), pv(r, "id"), pv(r, "ns"), pv(r, "key"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, e)
}

func (s *Server) boardPut(w http.ResponseWriter, r *http.Request) {
	body, ok := readBodyLimited(w, r, 1<<20)
	if !ok {
		return
	}
	var err error
	expected := int64(-1)
	if v := r.Header.Get("X-Expected-Version"); v != "" {
		expected, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad X-Expected-Version"})
			return
		}
	}
	e, err := s.svc.BoardPut(r.Context(), pv(r, "id"), pv(r, "ns"), pv(r, "key"), body, expected)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, e)
}

// ---- Artifact（M2） ----

func (s *Server) putArtifact(w http.ResponseWriter, r *http.Request) {
	missionID := r.Header.Get("X-Mission-ID")
	if missionID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "X-Mission-ID required"})
		return
	}
	if s.auth != nil {
		identity, _ := auth.FromContext(r.Context())
		if identity.Kind == "agent" && !s.requireSubtaskIdentity(w, r, r.Header.Get("X-Produced-By")) {
			return
		}
	}
	body, ok := readBodyLimited(w, r, 64<<20)
	if !ok {
		return
	}
	a, err := s.svc.PutArtifact(r.Context(), missionID,
		r.Header.Get("X-Produced-By"), r.Header.Get("X-Schema-Ref"), body)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, a)
}

func (s *Server) getArtifact(w http.ResponseWriter, r *http.Request) {
	a, err := s.svc.GetArtifact(r.Context(), pv(r, "id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) getArtifactContent(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeArtifactDownload(w, r, pv(r, "id")) {
		return
	}
	data, a, err := s.svc.GetArtifactContent(r.Context(), pv(r, "id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Artifact-SHA256", a.SHA256)
	_, _ = w.Write(data)
}
