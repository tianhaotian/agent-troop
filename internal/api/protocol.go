package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"agenttroop/internal/auth"
	"agenttroop/internal/core"
	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (s *Server) decodeRPC(w http.ResponseWriter, r *http.Request) (jsonRPCRequest, bool) {
	body, ok := readBodyLimited(w, r, 1<<20)
	if !ok {
		return jsonRPCRequest{}, false
	}
	var req jsonRPCRequest
	if json.Unmarshal(body, &req) != nil || req.JSONRPC != "2.0" || req.Method == "" {
		s.writeRPC(w, nil, nil, &jsonRPCError{Code: -32600, Message: "Invalid Request"})
		return jsonRPCRequest{}, false
	}
	return req, true
}

func (s *Server) writeRPC(w http.ResponseWriter, id json.RawMessage, result any, rpcErr *jsonRPCError) {
	w.Header().Set("Content-Type", "application/json")
	writeJSON(w, http.StatusOK, jsonRPCResponse{JSONRPC: "2.0", ID: id, Result: result, Error: rpcErr})
}

func identitySubject(r *http.Request) string {
	if identity, ok := auth.FromContext(r.Context()); ok {
		return identity.Subject
	}
	return "anonymous"
}

// ---- A2A JSON-RPC boundary ----

func (s *Server) agentCard(w http.ResponseWriter, r *http.Request) {
	security := []map[string]any{}
	securitySchemes := map[string]any{}
	if s.auth != nil {
		security = append(security, map[string]any{
			"schemes": map[string]any{"bearerAuth": map[string]any{"list": []string{}}},
		})
		securitySchemes["bearerAuth"] = map[string]any{
			"httpAuthSecurityScheme": map[string]string{"scheme": "Bearer", "bearerFormat": "TROOP"},
		}
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if forwarded := r.Header.Get("X-Forwarded-Proto"); forwarded == "http" || forwarded == "https" {
		scheme = forwarded
	}
	a2aURL := scheme + "://" + r.Host + "/a2a"
	writeJSON(w, http.StatusOK, map[string]any{
		"name": "Agent Troop", "description": "Auditable multi-agent mission control plane",
		"version":             "0.8.0",
		"supportedInterfaces": []map[string]any{{"url": a2aURL, "protocolBinding": "JSONRPC", "protocolVersion": "1.0"}},
		"capabilities":        map[string]bool{"streaming": false, "pushNotifications": false},
		"defaultInputModes":   []string{"text/plain", "application/json"},
		"defaultOutputModes":  []string{"application/json"},
		"skills": []map[string]any{{
			"id": "mission-orchestration", "name": "Mission orchestration",
			"description": "Create, inspect and cancel auditable multi-agent missions",
			"tags":        []string{"multi-agent", "workflow", "orchestration"},
		}},
		"securitySchemes": securitySchemes, "securityRequirements": security,
	})
}

type a2aMessage struct {
	MessageID string `json:"messageId,omitempty"`
	Role      string `json:"role,omitempty"`
	Parts     []struct {
		Kind string `json:"kind,omitempty"`
		Text string `json:"text,omitempty"`
		Data any    `json:"data,omitempty"`
	} `json:"parts"`
}

func (s *Server) a2aJSONRPC(w http.ResponseWriter, r *http.Request) {
	req, ok := s.decodeRPC(w, r)
	if !ok {
		return
	}
	switch req.Method {
	case "SendMessage", "message/send":
		var params struct {
			Message a2aMessage `json:"message"`
		}
		if json.Unmarshal(req.Params, &params) != nil {
			s.writeRPC(w, req.ID, nil, &jsonRPCError{Code: -32602, Message: "Invalid params"})
			return
		}
		goal := a2aMessageText(params.Message)
		if goal == "" {
			s.writeRPC(w, req.ID, nil, &jsonRPCError{Code: -32602, Message: "message must contain a text part"})
			return
		}
		owner := identitySubject(r)
		m, err := s.svc.CreateMissionWithBudgetAs(r.Context(), requestActor(r,
			store.Actor{Kind: "human", ID: owner}), owner, goal, 0, []core.TaskSpec{{
			Name: "a2a", Kind: mission.KindAgent, Input: map[string]any{"a2a_message": params.Message},
		}})
		if err != nil {
			s.writeRPC(w, req.ID, nil, a2aRPCError(err))
			return
		}
		task := a2aTask(m, nil)
		if req.Method == "SendMessage" {
			s.writeRPC(w, req.ID, map[string]any{"task": task}, nil)
		} else {
			s.writeRPC(w, req.ID, task, nil)
		}
	case "GetTask", "tasks/get":
		var params struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(req.Params, &params) != nil || params.ID == "" {
			s.writeRPC(w, req.ID, nil, &jsonRPCError{Code: -32602, Message: "id required"})
			return
		}
		m, err := s.svc.GetMission(r.Context(), params.ID)
		if err != nil {
			s.writeRPC(w, req.ID, nil, a2aRPCError(err))
			return
		}
		subs, _ := s.svc.ListSubtasks(r.Context(), params.ID)
		s.writeRPC(w, req.ID, a2aTask(m, subs), nil)
	case "CancelTask", "tasks/cancel":
		var params struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(req.Params, &params) != nil || params.ID == "" {
			s.writeRPC(w, req.ID, nil, &jsonRPCError{Code: -32602, Message: "id required"})
			return
		}
		if err := s.svc.CancelMissionAs(r.Context(), params.ID,
			requestActor(r, store.Actor{Kind: "human", ID: identitySubject(r)})); err != nil {
			s.writeRPC(w, req.ID, nil, a2aRPCError(err))
			return
		}
		m, _ := s.svc.GetMission(r.Context(), params.ID)
		s.writeRPC(w, req.ID, a2aTask(m, nil), nil)
	default:
		s.writeRPC(w, req.ID, nil, &jsonRPCError{Code: -32601, Message: "Method not found"})
	}
}

func a2aMessageText(message a2aMessage) string {
	var texts []string
	for _, part := range message.Parts {
		if (part.Kind == "" || part.Kind == "text") && strings.TrimSpace(part.Text) != "" {
			texts = append(texts, strings.TrimSpace(part.Text))
		}
	}
	return strings.Join(texts, "\n")
}

func a2aTask(m *mission.Mission, subs []*mission.Subtask) map[string]any {
	state := "TASK_STATE_WORKING"
	switch m.Status {
	case mission.MissionSucceeded:
		state = "TASK_STATE_COMPLETED"
	case mission.MissionFailed:
		state = "TASK_STATE_FAILED"
	case mission.MissionCancelled:
		state = "TASK_STATE_CANCELED"
	}
	result := map[string]any{"id": m.ID, "contextId": m.ID, "status": map[string]any{"state": state}}
	if subs != nil {
		result["metadata"] = map[string]any{"mission": m, "subtasks": subs}
	}
	return result
}

// ---- MCP Streamable HTTP JSON-RPC boundary ----

func (s *Server) mcpJSONRPC(w http.ResponseWriter, r *http.Request) {
	req, ok := s.decodeRPC(w, r)
	if !ok {
		return
	}
	protocolVersion := mcpRequestVersion(req.Params)
	if protocolVersion != "" && !supportedMCPVersion(protocolVersion) {
		s.writeRPC(w, req.ID, nil, &jsonRPCError{Code: -32022, Message: "Unsupported protocol version"})
		return
	}
	if protocolVersion == "2026-07-28" && r.Header.Get("Mcp-Method") != req.Method {
		s.writeRPC(w, req.ID, nil, &jsonRPCError{Code: -32020, Message: "Mcp-Method header mismatch"})
		return
	}
	if protocolVersion == "2026-07-28" {
		if name := mcpRequestName(req.Method, req.Params); name != "" && r.Header.Get("Mcp-Name") != name {
			s.writeRPC(w, req.ID, nil, &jsonRPCError{Code: -32020, Message: "Mcp-Name header mismatch"})
			return
		}
	}
	if len(req.ID) == 0 && strings.HasPrefix(req.Method, "notifications/") {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	switch req.Method {
	case "server/discover":
		s.writeRPC(w, req.ID, map[string]any{
			"resultType": "complete", "supportedVersions": supportedMCPVersions,
			"capabilities": mcpCapabilities(), "instructions": "Use Agent Troop tools to create and inspect auditable missions.",
			"ttlMs": 300000, "cacheScope": "public", "_meta": mcpServerMeta(),
		}, nil)
	case "initialize":
		var params struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		_ = json.Unmarshal(req.Params, &params)
		version := supportedMCPVersions[0]
		if params.ProtocolVersion != "" {
			if !supportedMCPVersion(params.ProtocolVersion) {
				s.writeRPC(w, req.ID, nil, &jsonRPCError{Code: -32022, Message: "Unsupported protocol version"})
				return
			}
			version = params.ProtocolVersion
		}
		s.writeRPC(w, req.ID, map[string]any{
			"resultType": "complete", "protocolVersion": version,
			"capabilities": mcpCapabilities(),
			"serverInfo":   map[string]string{"name": "agent-troop", "version": "0.8.0"},
			"_meta":        mcpServerMeta(),
		}, nil)
	case "ping":
		s.writeRPC(w, req.ID, map[string]any{"resultType": "complete", "_meta": mcpServerMeta()}, nil)
	case "tools/list":
		s.writeRPC(w, req.ID, map[string]any{"resultType": "complete", "tools": mcpTools(),
			"ttlMs": 300000, "cacheScope": "private", "_meta": mcpServerMeta()}, nil)
	case "tools/call":
		s.mcpCallTool(w, r, req)
	case "resources/list":
		s.writeRPC(w, req.ID, map[string]any{"resultType": "complete", "resources": []any{},
			"ttlMs": 60000, "cacheScope": "private", "_meta": mcpServerMeta()}, nil)
	case "resources/templates/list":
		s.writeRPC(w, req.ID, map[string]any{"resultType": "complete", "resourceTemplates": []map[string]any{{
			"uriTemplate": "troop://missions/{id}", "name": "mission", "description": "Mission and subtask projection", "mimeType": "application/json",
		}}, "ttlMs": 300000, "cacheScope": "private", "_meta": mcpServerMeta()}, nil)
	case "resources/read":
		var params struct {
			URI string `json:"uri"`
		}
		if json.Unmarshal(req.Params, &params) != nil {
			s.writeRPC(w, req.ID, nil, &jsonRPCError{Code: -32602, Message: "Invalid params"})
			return
		}
		id, err := missionIDFromURI(params.URI)
		if err != nil {
			s.writeRPC(w, req.ID, nil, &jsonRPCError{Code: -32602, Message: err.Error()})
			return
		}
		projection, err := s.missionProjection(r, id)
		if err != nil {
			s.writeRPC(w, req.ID, nil, mcpRPCError(err))
			return
		}
		encoded, _ := json.Marshal(projection)
		s.writeRPC(w, req.ID, map[string]any{"resultType": "complete", "contents": []map[string]any{{
			"uri": params.URI, "mimeType": "application/json", "text": string(encoded),
		}}, "ttlMs": 1000, "cacheScope": "private", "_meta": mcpServerMeta()}, nil)
	default:
		s.writeRPC(w, req.ID, nil, &jsonRPCError{Code: -32601, Message: "Method not found"})
	}
}

var supportedMCPVersions = []string{"2026-07-28", "2025-11-25", "2025-06-18", "2025-03-26"}

func supportedMCPVersion(version string) bool {
	for _, supported := range supportedMCPVersions {
		if version == supported {
			return true
		}
	}
	return false
}

func mcpRequestVersion(params json.RawMessage) string {
	var envelope struct {
		Meta map[string]any `json:"_meta"`
	}
	if json.Unmarshal(params, &envelope) != nil {
		return ""
	}
	version, _ := envelope.Meta["io.modelcontextprotocol/protocolVersion"].(string)
	return version
}

func mcpRequestName(method string, params json.RawMessage) string {
	var envelope map[string]any
	if json.Unmarshal(params, &envelope) != nil {
		return ""
	}
	switch method {
	case "tools/call", "prompts/get":
		name, _ := envelope["name"].(string)
		return name
	case "resources/read":
		uri, _ := envelope["uri"].(string)
		return uri
	default:
		return ""
	}
}

func mcpCapabilities() map[string]any {
	return map[string]any{"tools": map[string]bool{"listChanged": false}, "resources": map[string]bool{"listChanged": false}}
}

func mcpServerMeta() map[string]any {
	return map[string]any{"io.modelcontextprotocol/serverInfo": map[string]string{"name": "agent-troop", "version": "0.8.0"}}
}

func mcpTools() []map[string]any {
	return []map[string]any{
		{"name": "troop.create_mission", "description": "Create an auditable mission", "inputSchema": map[string]any{
			"type": "object", "required": []string{"goal", "tasks"}, "properties": map[string]any{
				"owner": map[string]string{"type": "string"}, "goal": map[string]string{"type": "string"},
				"budget_tokens": map[string]string{"type": "integer"}, "tasks": map[string]any{"type": "array", "items": map[string]string{"type": "object"}},
			},
		}},
		{"name": "troop.get_mission", "description": "Read a mission projection", "inputSchema": idSchema()},
		{"name": "troop.cancel_mission", "description": "Cancel an active mission", "inputSchema": idSchema()},
		{"name": "troop.wake_subtask", "description": "Wake a waiting subtask", "inputSchema": map[string]any{
			"type": "object", "required": []string{"subtask_id"}, "properties": map[string]any{"subtask_id": map[string]string{"type": "string"}},
		}},
	}
}

func idSchema() map[string]any {
	return map[string]any{"type": "object", "required": []string{"id"}, "properties": map[string]any{"id": map[string]string{"type": "string"}}}
}

func (s *Server) mcpCallTool(w http.ResponseWriter, r *http.Request, req jsonRPCRequest) {
	var params struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if json.Unmarshal(req.Params, &params) != nil || params.Name == "" {
		s.writeRPC(w, req.ID, nil, &jsonRPCError{Code: -32602, Message: "Invalid params"})
		return
	}
	var value any
	var err error
	switch params.Name {
	case "troop.create_mission":
		var args struct {
			Owner        string          `json:"owner"`
			Goal         string          `json:"goal"`
			BudgetTokens int64           `json:"budget_tokens,omitempty"`
			Tasks        []core.TaskSpec `json:"tasks"`
		}
		if json.Unmarshal(params.Arguments, &args) != nil || args.Goal == "" || len(args.Tasks) == 0 {
			err = errors.New("goal and tasks required")
			break
		}
		if args.Owner == "" {
			args.Owner = identitySubject(r)
		}
		value, err = s.svc.CreateMissionWithBudgetAs(r.Context(), requestActor(r,
			store.Actor{Kind: "human", ID: args.Owner}), args.Owner, args.Goal, args.BudgetTokens, args.Tasks)
	case "troop.get_mission":
		var args struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(params.Arguments, &args)
		value, err = s.missionProjection(r, args.ID)
	case "troop.cancel_mission":
		var args struct {
			ID string `json:"id"`
		}
		_ = json.Unmarshal(params.Arguments, &args)
		err = s.svc.CancelMissionAs(r.Context(), args.ID,
			requestActor(r, store.Actor{Kind: "human", ID: identitySubject(r)}))
		value = map[string]string{"id": args.ID, "status": "CANCELLED"}
	case "troop.wake_subtask":
		var args struct {
			SubtaskID string `json:"subtask_id"`
		}
		_ = json.Unmarshal(params.Arguments, &args)
		value, err = s.svc.WakeAs(r.Context(), args.SubtaskID,
			requestActor(r, store.Actor{Kind: "human", ID: identitySubject(r)}))
	default:
		s.writeRPC(w, req.ID, nil, &jsonRPCError{Code: -32602, Message: "Unknown tool"})
		return
	}
	if err != nil {
		s.writeRPC(w, req.ID, map[string]any{"resultType": "complete", "content": []map[string]string{{"type": "text", "text": err.Error()}}, "isError": true, "_meta": mcpServerMeta()}, nil)
		return
	}
	encoded, _ := json.Marshal(value)
	s.writeRPC(w, req.ID, map[string]any{"resultType": "complete", "content": []map[string]string{{"type": "text", "text": string(encoded)}}, "structuredContent": value, "_meta": mcpServerMeta()}, nil)
}

func (s *Server) missionProjection(r *http.Request, id string) (map[string]any, error) {
	if id == "" {
		return nil, errors.New("id required")
	}
	m, err := s.svc.GetMission(r.Context(), id)
	if err != nil {
		return nil, err
	}
	subs, err := s.svc.ListSubtasks(r.Context(), id)
	if err != nil {
		return nil, err
	}
	return map[string]any{"mission": m, "subtasks": subs}, nil
}

func missionIDFromURI(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "troop" || u.Host != "missions" {
		return "", fmt.Errorf("unsupported resource URI")
	}
	id := strings.TrimPrefix(u.Path, "/")
	if id == "" || strings.Contains(id, "/") {
		return "", fmt.Errorf("mission id required")
	}
	return id, nil
}

func a2aRPCError(err error) *jsonRPCError {
	code := -32603
	if errors.Is(err, store.ErrNotFound) {
		code = -32001
	} else if errors.Is(err, store.ErrConflict) {
		code = -32002
	} else if errors.Is(err, core.ErrInvalidDAG) || errors.Is(err, core.ErrInvalidBudget) {
		code = -32602
	}
	return &jsonRPCError{Code: code, Message: err.Error()}
}

func mcpRPCError(err error) *jsonRPCError {
	code := -32603
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, core.ErrInvalidDAG) || errors.Is(err, core.ErrInvalidBudget) {
		code = -32602
	}
	return &jsonRPCError{Code: code, Message: err.Error()}
}
