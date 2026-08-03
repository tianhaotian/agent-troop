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

	"agenttroop/internal/core"
	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

type Server struct {
	svc *core.Service
}

func New(svc *core.Service) *Server { return &Server{svc: svc} }

func (s *Server) Handler() http.Handler {
	mux := newRouter()
	mux.handle("GET /healthz", s.healthz)

	// 任务面
	mux.handle("POST /v1/missions", s.createMission)
	mux.handle("GET /v1/missions/{id}", s.getMission)
	mux.handle("POST /v1/missions/{id}/cancel", s.cancelMission)
	mux.handle("GET /v1/missions/{id}/events", s.missionEventsSSE)

	// Agent 面
	mux.handle("POST /v1/agents/register", s.registerAgent)
	mux.handle("POST /v1/agents/{id}/heartbeat", s.heartbeat)
	mux.handle("GET /v1/agents/{id}/offers", s.listOffers)

	// 执行面（Agent 回调）
	mux.handle("POST /v1/leases/{id}/accept", s.acceptLease)
	mux.handle("POST /v1/subtasks/{id}/start", s.startSubtask)
	mux.handle("POST /v1/subtasks/{id}/progress", s.progress)
	mux.handle("POST /v1/subtasks/{id}/complete", s.completeSubtask)
	mux.handle("POST /v1/subtasks/{id}/fail", s.failSubtask)

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

	// 最小 Console（S11）
	mux.handle("GET /", s.console)
	return mux
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

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---- 任务面 ----

type createMissionReq struct {
	Owner string           `json:"owner"`
	Goal  string           `json:"goal"`
	Tasks []core.TaskSpec  `json:"tasks"`
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
	m, err := s.svc.CreateMission(r.Context(), req.Owner, req.Goal, req.Tasks)
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

func (s *Server) cancelMission(w http.ResponseWriter, r *http.Request) {
	id := pv(r, "id")
	var body struct {
		Owner string `json:"owner"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.Owner == "" {
		body.Owner = "anonymous"
	}
	if err := s.svc.CancelMission(r.Context(), id, body.Owner); err != nil {
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
				fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", e.Seq, e.Type, data)
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
	}
	if err := s.svc.RegisterAgent(r.Context(), a); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	if err := s.svc.Heartbeat(r.Context(), pv(r, "id")); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type offerView struct {
	Subtask      *mission.Subtask `json:"subtask"`
	LeaseID      string           `json:"lease_id"`
	FencingToken int64            `json:"fencing_token"`
}

func (s *Server) listOffers(w http.ResponseWriter, r *http.Request) {
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
		views = append(views, offerView{Subtask: sub, LeaseID: l.ID, FencingToken: l.FencingToken})
	}
	writeJSON(w, http.StatusOK, map[string]any{"offers": views})
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
	sub, err := s.svc.StartSubtask(r.Context(), pv(r, "id"), req.FencingToken, req.Version, req.AgentID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

type progressReq struct {
	LeaseID      string `json:"lease_id"`
	FencingToken int64  `json:"fencing_token"`
}

func (s *Server) progress(w http.ResponseWriter, r *http.Request) {
	var req progressReq
	if !decode(w, r, &req) {
		return
	}
	if err := s.svc.RenewLease(r.Context(), req.LeaseID, req.FencingToken); err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "renewed"})
}

type completeReq struct {
	AgentID        string `json:"agent_id"`
	FencingToken   int64  `json:"fencing_token"`
	Version        int64  `json:"version"`
	IdempotencyKey string `json:"idempotency_key"`
	ResultRef      string `json:"result_ref"`
}

func (s *Server) completeSubtask(w http.ResponseWriter, r *http.Request) {
	var req completeReq
	if !decode(w, r, &req) {
		return
	}
	sub, err := s.svc.CompleteSubtask(r.Context(), pv(r, "id"),
		req.FencingToken, req.IdempotencyKey, req.ResultRef, req.Version, req.AgentID)
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
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
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
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
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
	data, a, err := s.svc.GetArtifactContent(r.Context(), pv(r, "id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Artifact-SHA256", a.SHA256)
	_, _ = w.Write(data)
}
