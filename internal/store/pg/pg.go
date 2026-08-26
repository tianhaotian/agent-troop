// Package pg 提供 Store 的 PostgreSQL 实现（PG-first，设计 §22.4）。
//
// 语义与 memory 实现对齐：条件更新做乐观锁 CAS；迁移与事件追加同事务；
// fencing token 取全局序列 fencing_seq；leases 唯一部分索引兜底单活跃租约。
//
// 测试：需本地 PG（docker compose up -d postgres），
// 设置 TROOP_TEST_PG=postgres://troop:troop@localhost:5432/troop 后运行。
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func Connect(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pg: connect: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// ---- helpers ----

func js(v any) ([]byte, error) { return json.Marshal(v) }

// textArray keeps nil Go slices from becoming SQL NULL. PostgreSQL array
// columns such as subtasks.depends_on are NOT NULL; an omitted dependency list
// therefore has to cross the persistence boundary as an empty array.
func textArray(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}

func scanSubtask(row pgx.Row) (*mission.Subtask, error) {
	var sub mission.Subtask
	var kind, state string
	var spec, scheduling, retry []byte
	var parentID, assignee, leaseID, resultRef, wakeKind *string
	var dependsOn []string
	err := row.Scan(&sub.ID, &sub.MissionID, &parentID, &kind, &spec, &scheduling, &retry,
		&state, &dependsOn, &assignee, &leaseID, &sub.Attempt, &resultRef, &sub.Version,
		&sub.Checkpoint, &wakeKind, &sub.WakeAt, &sub.WakeDeadline, &sub.WakeSpec)
	if err != nil {
		return nil, err
	}
	if wakeKind != nil {
		sub.WakeKind = *wakeKind
	}
	sub.Kind = mission.Kind(kind)
	sub.State = mission.State(state)
	sub.DependsOn = dependsOn
	if parentID != nil {
		sub.ParentID = *parentID
	}
	if assignee != nil {
		sub.Assignee = *assignee
	}
	if leaseID != nil {
		sub.LeaseID = *leaseID
	}
	if resultRef != nil {
		sub.ResultRef = *resultRef
	}
	var specObj struct {
		RequiredSkills []string                   `json:"required_skills"`
		Question       string                     `json:"question"`
		Options        []string                   `json:"options"`
		OnTimeout      string                     `json:"on_timeout"`
		Input          map[string]any             `json:"input"`     // M6：delegate 子女任务载荷
		ReworkOf       string                     `json:"rework_of"` // M6：rework 链
		Grants         mission.PermissionEnvelope `json:"grants"`    // M7D：权限包络
	}
	_ = json.Unmarshal(spec, &specObj)
	sub.RequiredSkills = specObj.RequiredSkills
	sub.Question = specObj.Question
	sub.Options = specObj.Options
	sub.OnTimeout = specObj.OnTimeout
	sub.Input = specObj.Input
	sub.ReworkOf = specObj.ReworkOf
	sub.Grants = specObj.Grants
	_ = json.Unmarshal(scheduling, &sub.Scheduling)
	_ = json.Unmarshal(retry, &sub.Retry)
	return &sub, nil
}

// marshalSpec 序列化 subtask spec JSONB（required_skills + human 节点字段 + M6 委托字段）。
func marshalSpec(sub *mission.Subtask) []byte {
	spec, _ := json.Marshal(map[string]any{
		"required_skills": sub.RequiredSkills,
		"question":        sub.Question,
		"options":         sub.Options,
		"on_timeout":      sub.OnTimeout,
		"input":           sub.Input,
		"rework_of":       sub.ReworkOf,
		"grants":          sub.Grants,
	})
	return spec
}

const subtaskCols = `id, mission_id, parent_id, kind, spec, scheduling, retry, state,
	depends_on, assignee_agent_id, lease_id, attempt, result_ref, version,
	checkpoint, wake_kind, wake_at, wake_deadline, wake_spec`

func appendEvent(ctx context.Context, tx pgx.Tx, e *store.Event) error {
	payload, err := js(e.Payload)
	if err != nil {
		return err
	}
	actor, err := js(e.Actor)
	if err != nil {
		return err
	}
	return tx.QueryRow(ctx,
		`INSERT INTO events (aggregate_id, mission_id, type, payload, actor, ts)
		 VALUES ($1,$2,$3,$4,$5,$6) RETURNING seq`,
		e.AggregateID, e.MissionID, e.Type, payload, actor, e.Ts).Scan(&e.Seq)
}

// AppendEvent 追加独立事件（M5：condition.cost_exceeded 等平台留痕；
// 不与状态迁移同事务，单独短事务提交）。
func (s *Store) AppendEvent(ctx context.Context, e *store.Event) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := appendEvent(ctx, tx, e); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ---- 任务面 ----

func (s *Store) CreateMission(ctx context.Context, m *mission.Mission, subs []*mission.Subtask, actor store.Actor, now time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`INSERT INTO missions (id, owner, goal, constraints, status, version, created_at, updated_at)
		 VALUES ($1,$2,$3,'{}',$4,0,$5,$5)`, m.ID, m.Owner, m.Goal, string(m.Status), now); err != nil {
		return store.ErrConflict
	}
	if m.BudgetTokens > 0 {
		if _, err := tx.Exec(ctx,
			`INSERT INTO mission_budgets
			 (mission_id, total_tokens, held_tokens, spent_tokens, version, updated_at)
			 VALUES ($1,$2,0,0,0,$3)`, m.ID, m.BudgetTokens, now); err != nil {
			return err
		}
	}
	ev := &store.Event{AggregateID: m.ID, MissionID: m.ID, Type: "mission.created",
		Payload: map[string]any{"goal": m.Goal, "owner": m.Owner, "budget_tokens": m.BudgetTokens}, Actor: actor, Ts: now}
	if err := appendEvent(ctx, tx, ev); err != nil {
		return err
	}
	for _, sub := range subs {
		spec := marshalSpec(sub)
		scheduling, _ := js(sub.Scheduling)
		retry, _ := js(sub.Retry)
		if _, err := tx.Exec(ctx,
			`INSERT INTO subtasks (id, mission_id, parent_id, kind, spec, scheduling, retry, state,
			 depends_on, attempt, version, created_at, updated_at)
			 VALUES ($1,$2,NULLIF($3,''),$4,$5,$6,$7,$8,$9,$10,0,$11,$11)`,
			sub.ID, sub.MissionID, sub.ParentID, string(sub.Kind), spec, scheduling, retry,
			string(sub.State), textArray(sub.DependsOn), sub.Attempt, now); err != nil {
			return err
		}
		ev := &store.Event{AggregateID: sub.ID, MissionID: sub.MissionID, Type: string(mission.EvCreated),
			Payload: map[string]any{"kind": string(sub.Kind)}, Actor: actor, Ts: now}
		if err := appendEvent(ctx, tx, ev); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) GetMission(ctx context.Context, id string) (*mission.Mission, error) {
	var m mission.Mission
	var status string
	err := s.pool.QueryRow(ctx,
		`SELECT m.id, m.owner, m.goal, COALESCE(b.total_tokens,0), m.status, m.version
		 FROM missions m LEFT JOIN mission_budgets b ON b.mission_id=m.id WHERE m.id=$1`, id).
		Scan(&m.ID, &m.Owner, &m.Goal, &m.BudgetTokens, &status, &m.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	m.Status = mission.MissionStatus(status)
	return &m, nil
}

func (s *Store) GetMissionBudget(ctx context.Context, missionID string) (*store.BudgetAccount, error) {
	var account store.BudgetAccount
	var exists bool
	var updatedAt *time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT m.id, b.mission_id IS NOT NULL, COALESCE(b.total_tokens,0),
		 COALESCE(b.held_tokens,0), COALESCE(b.spent_tokens,0), COALESCE(b.version,0), b.updated_at
		 FROM missions m LEFT JOIN mission_budgets b ON b.mission_id=m.id WHERE m.id=$1`, missionID).
		Scan(&account.MissionID, &exists, &account.Total, &account.Held, &account.Spent,
			&account.Version, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	account.Metered = exists
	account.Available = account.Total - account.Held - account.Spent
	if updatedAt != nil {
		account.UpdatedAt = *updatedAt
	}
	return &account, nil
}

func (s *Store) ListBudgetHolds(ctx context.Context, missionID string) ([]*store.BudgetHold, error) {
	if _, err := s.GetMissionBudget(ctx, missionID); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, mission_id, subtask_id, attempt, amount_tokens, actual_tokens,
		 status, created_at, settled_at
		 FROM budget_holds WHERE mission_id=$1 ORDER BY created_at, id`, missionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.BudgetHold
	for rows.Next() {
		var hold store.BudgetHold
		if err := rows.Scan(&hold.ID, &hold.MissionID, &hold.SubtaskID, &hold.Attempt,
			&hold.Amount, &hold.Actual, &hold.Status, &hold.CreatedAt, &hold.SettledAt); err != nil {
			return nil, err
		}
		out = append(out, &hold)
	}
	return out, rows.Err()
}

func (s *Store) ListSubtasks(ctx context.Context, missionID string) ([]*mission.Subtask, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE mission_id=$1 ORDER BY id`, missionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*mission.Subtask
	for rows.Next() {
		sub, err := scanSubtask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

func (s *Store) ListSubtasksByState(ctx context.Context, st mission.State) ([]*mission.Subtask, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE state=$1 ORDER BY id`, string(st))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*mission.Subtask
	for rows.Next() {
		sub, err := scanSubtask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

func (s *Store) SetMissionStatus(ctx context.Context, id string, st mission.MissionStatus, expectedVersion int64, actor store.Actor, now time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx,
		`UPDATE missions SET status=$1, version=version+1, updated_at=$2
		 WHERE id=$3 AND version=$4`, string(st), now, id, expectedVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrConflict
	}
	ev := &store.Event{AggregateID: id, MissionID: id, Type: "mission.state_changed",
		Payload: map[string]any{"status": string(st)}, Actor: actor, Ts: now}
	if err := appendEvent(ctx, tx, ev); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) TransitionSubtask(ctx context.Context, id string, evName mission.EventType, expectedVersion int64,
	actor store.Actor, payload map[string]any, now time.Time,
	mutate func(*mission.Subtask) error) (*mission.Subtask, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	sub, err := scanSubtask(tx.QueryRow(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE id=$1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	next, err := mission.Apply(sub.State, evName)
	if err != nil {
		return nil, store.ErrConflict
	}
	sub.State = next
	if mutate != nil {
		if err := mutate(sub); err != nil {
			return nil, err
		}
	}
	if err := updateSubtask(ctx, tx, sub, expectedVersion, now); err != nil {
		return nil, err
	}
	if payload == nil {
		payload = map[string]any{}
	}
	payload["state"] = string(next)
	ev := &store.Event{AggregateID: sub.ID, MissionID: sub.MissionID, Type: string(evName),
		Payload: payload, Actor: actor, Ts: now}
	if err := appendEvent(ctx, tx, ev); err != nil {
		return nil, err
	}
	sub.Version++
	return sub, tx.Commit(ctx)
}

func updateSubtask(ctx context.Context, tx pgx.Tx, sub *mission.Subtask, expectedVersion int64, now time.Time) error {
	spec := marshalSpec(sub)
	scheduling, _ := js(sub.Scheduling)
	retry, _ := js(sub.Retry)
	tag, err := tx.Exec(ctx,
		`UPDATE subtasks SET state=$1, assignee_agent_id=NULLIF($2,''), lease_id=NULLIF($3,''),
		 attempt=$4, result_ref=NULLIF($5,''), spec=$6, scheduling=$7, retry=$8,
		 checkpoint=$10, wake_kind=NULLIF($11,''), wake_at=$12, wake_deadline=$13, wake_spec=$16,
		 version=version+1, updated_at=$9
		 WHERE id=$14 AND version=$15`,
		string(sub.State), sub.Assignee, sub.LeaseID, sub.Attempt, sub.ResultRef,
		spec, scheduling, retry, now, sub.Checkpoint, sub.WakeKind, sub.WakeAt, sub.WakeDeadline,
		sub.ID, expectedVersion, sub.WakeSpec)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrConflict
	}
	return nil
}

// ---- 就绪队列 ----

func (s *Store) DequeueReady(ctx context.Context, limit int) ([]*mission.Subtask, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE state='READY'
		 ORDER BY (scheduling->>'priority')::int DESC NULLS LAST,
		          (scheduling->>'deadline')::timestamptz ASC NULLS LAST, id
		 LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*mission.Subtask
	for rows.Next() {
		sub, err := scanSubtask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

// ---- Agent 注册 ----

func (s *Store) UpsertAgent(ctx context.Context, a *store.Agent, now time.Time) error {
	caps, _ := js(a.Capabilities)
	endpoint, _ := js(a.Endpoint)
	health := a.Health
	if health == "" {
		health = "healthy"
	}
	scopes, _ := js(a.TriggerScopes)
	if a.TriggerScopes == nil {
		scopes = []byte("[]") // 列 NOT NULL：未声明即默认收紧（空授权）
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO agents (id, name, platform, endpoint, capabilities, constraints, health, trigger_scopes, auth_subject, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,''),$10,$10)
		 ON CONFLICT (id) DO UPDATE SET name=$2, platform=$3, endpoint=$4, capabilities=$5,
		 constraints=$6, health=$7, trigger_scopes=$8, auth_subject=NULLIF($9,''), updated_at=$10, version=agents.version+1`,
		a.ID, a.Name, a.Platform, endpoint, caps,
		fmt.Sprintf(`{"max_concurrency":%d}`, a.MaxConcurrency),
		fmt.Sprintf(`{"status":%q,"last_heartbeat":%q}`, health, now.Format(time.RFC3339Nano)),
		scopes, a.AuthSubject, now)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return store.ErrConflict
	}
	return err
}

func scanAgent(row pgx.Row) (*store.Agent, error) {
	var a store.Agent
	var caps, endpoint, constraints, health, scopes []byte
	var authSubject *string
	err := row.Scan(&a.ID, &a.Name, &a.Platform, &endpoint, &caps, &constraints, &health, &scopes,
		&authSubject, &a.Running)
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal(caps, &a.Capabilities)
	_ = json.Unmarshal(endpoint, &a.Endpoint)
	_ = json.Unmarshal(scopes, &a.TriggerScopes)
	if authSubject != nil {
		a.AuthSubject = *authSubject
	}
	var c struct {
		MaxConcurrency int `json:"max_concurrency"`
	}
	_ = json.Unmarshal(constraints, &c)
	a.MaxConcurrency = c.MaxConcurrency
	var h struct {
		Status        string    `json:"status"`
		LastHeartbeat time.Time `json:"last_heartbeat"`
	}
	_ = json.Unmarshal(health, &h)
	a.Health = h.Status
	a.LastHeartbeat = h.LastHeartbeat
	return &a, nil
}

const agentCols = `a.id, a.name, a.platform, a.endpoint, a.capabilities, a.constraints, a.health,
	a.trigger_scopes, a.auth_subject,
	(SELECT count(*) FROM leases l WHERE l.agent_id=a.id AND l.state='ACTIVE') AS running`

func (s *Store) GetAgent(ctx context.Context, id string) (*store.Agent, error) {
	a, err := scanAgent(s.pool.QueryRow(ctx,
		`SELECT `+agentCols+` FROM agents a WHERE a.id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return a, err
}

func (s *Store) ListAgents(ctx context.Context) ([]*store.Agent, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+agentCols+` FROM agents a ORDER BY a.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.Agent
	for rows.Next() {
		a, err := scanAgent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) HeartbeatAgent(ctx context.Context, id string, now time.Time) error {
	// 心跳即存活证明：suspect 自动恢复 healthy（down 不自动恢复，与 memory 实现一致）
	tag, err := s.pool.Exec(ctx,
		`UPDATE agents SET health=jsonb_set(
		   jsonb_set(health,'{last_heartbeat}',to_jsonb($2::timestamptz)),
		   '{status}', to_jsonb(CASE WHEN health->>'status'='suspect' THEN 'healthy'
		                             ELSE health->>'status' END)),
		 updated_at=$2 WHERE id=$1`, id, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (s *Store) MarkAgentHealth(ctx context.Context, id, health string) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE agents SET health=jsonb_set(health,'{status}',to_jsonb($2::text)) WHERE id=$1`, id, health)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrNotFound
	}
	return nil
}

func (s *Store) ListReputations(ctx context.Context, agentID string) ([]*store.ReputationRecord, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM agents WHERE id=$1)`, agentID).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, store.ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `SELECT agent_id, skill, success_alpha, success_beta,
		quality_ewma, quality_samples, reliability_alpha, reliability_beta,
		latency_ewma_ms, cost_efficiency, samples, updated_at
		FROM reputations WHERE agent_id=$1 ORDER BY skill`, agentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.ReputationRecord
	for rows.Next() {
		r, err := scanReputation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func scanReputation(row pgx.Row) (*store.ReputationRecord, error) {
	var r store.ReputationRecord
	if err := row.Scan(&r.AgentID, &r.Skill, &r.SuccessAlpha, &r.SuccessBeta,
		&r.QualityEWMA, &r.QualitySamples, &r.ReliabilityAlpha, &r.ReliabilityBeta,
		&r.LatencyEWMAms, &r.CostEfficiency, &r.Samples, &r.UpdatedAt); err != nil {
		return nil, err
	}
	r.RefreshScores()
	return &r, nil
}

func (s *Store) ApplyReputationSignal(ctx context.Context, sig store.ReputationSignal, now time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := applyReputationSignalTx(ctx, tx, sig, now); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func applyReputationSignalTx(ctx context.Context, tx pgx.Tx, sig store.ReputationSignal, now time.Time) error {
	payload, err := js(sig)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO reputation_signals
		(id, agent_id, skill, signal, event_ref, created_at) VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (id) DO NOTHING`, sig.ID, sig.AgentID, sig.Skill, payload, sig.EventRef, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrDuplicate
	}
	r, err := scanReputation(tx.QueryRow(ctx, `SELECT agent_id, skill, success_alpha, success_beta,
		quality_ewma, quality_samples, reliability_alpha, reliability_beta,
		latency_ewma_ms, cost_efficiency, samples, updated_at
		FROM reputations WHERE agent_id=$1 AND skill=$2 FOR UPDATE`, sig.AgentID, sig.Skill))
	if errors.Is(err, pgx.ErrNoRows) {
		r = store.NewReputation(sig.AgentID, sig.Skill)
	} else if err != nil {
		return err
	}
	store.ApplyReputationSignal(r, sig, now)
	_, err = tx.Exec(ctx, `INSERT INTO reputations
		(agent_id, skill, success_alpha, success_beta, quality_ewma, quality_samples,
		reliability_alpha, reliability_beta, latency_ewma_ms, cost_efficiency, samples, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (agent_id, skill) DO UPDATE SET success_alpha=$3, success_beta=$4,
		quality_ewma=$5, quality_samples=$6, reliability_alpha=$7, reliability_beta=$8,
		latency_ewma_ms=$9, cost_efficiency=$10, samples=$11, updated_at=$12`,
		r.AgentID, r.Skill, r.SuccessAlpha, r.SuccessBeta, r.QualityEWMA, r.QualitySamples,
		r.ReliabilityAlpha, r.ReliabilityBeta, r.LatencyEWMAms, r.CostEfficiency, r.Samples, r.UpdatedAt)
	return err
}

// ---- 租约 ----

func (s *Store) OfferLease(ctx context.Context, subtaskID, agentID string, expectedVersion int64,
	ttl time.Duration, actor store.Actor, now time.Time) (*store.Lease, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	sub, err := scanSubtask(tx.QueryRow(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE id=$1 FOR UPDATE`, subtaskID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	if _, err := mission.Apply(sub.State, mission.EvLeaseOffered); err != nil {
		return nil, store.ErrConflict
	}

	var lease store.Lease
	var fence int64
	if err := tx.QueryRow(ctx, `SELECT nextval('fencing_seq')`).Scan(&fence); err != nil {
		return nil, err
	}
	err = tx.QueryRow(ctx,
		`INSERT INTO leases (id, subtask_id, agent_id, fencing_token, expires_at, state, created_at)
		 VALUES ('les_' || $1, $2, $3, $4, $5, 'ACTIVE', $6) RETURNING id`,
		fmt.Sprintf("%012d", fence), subtaskID, agentID, fence, now.Add(ttl), now).Scan(&lease.ID)
	if err != nil {
		return nil, store.ErrConflict // 唯一部分索引：已有活跃租约
	}
	lease.SubtaskID, lease.AgentID, lease.FencingToken = subtaskID, agentID, fence
	lease.ExpiresAt, lease.State, lease.CreatedAt = now.Add(ttl), store.LeaseActive, now

	sub.State = mission.StateOffered
	sub.Assignee = agentID
	sub.LeaseID = lease.ID
	if err := updateSubtask(ctx, tx, sub, expectedVersion, now); err != nil {
		return nil, err
	}
	pkg, err := buildContextPackageTx(ctx, tx, lease.ID, sub, now)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(pkg)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO context_packages
		 (id, lease_id, mission_id, subtask_id, payload, snapshot_hash, created_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7)`, pkg.ID, lease.ID, sub.MissionID, sub.ID,
		payload, pkg.SnapshotHash, now); err != nil {
		return nil, err
	}
	ev := &store.Event{AggregateID: subtaskID, MissionID: sub.MissionID, Type: string(mission.EvLeaseOffered),
		Payload: map[string]any{"state": string(mission.StateOffered), "agent_id": agentID,
			"lease_id": lease.ID, "fencing_token": fence}, Actor: actor, Ts: now}
	if err := appendEvent(ctx, tx, ev); err != nil {
		return nil, err
	}
	if err := appendEvent(ctx, tx, &store.Event{AggregateID: sub.ID, MissionID: sub.MissionID,
		Type: "context.materialized", Payload: map[string]any{
			"context_package_id": pkg.ID, "lease_id": lease.ID, "snapshot_hash": pkg.SnapshotHash,
		}, Actor: store.Actor{Kind: "system", ID: "context-builder"}, Ts: now}); err != nil {
		return nil, err
	}
	return &lease, tx.Commit(ctx)
}

func buildContextPackageTx(ctx context.Context, tx pgx.Tx, leaseID string, sub *mission.Subtask,
	now time.Time) (*store.ContextPackage, error) {
	var artifacts []*store.Artifact
	if len(sub.Grants.ArtifactRefs) > 0 {
		rows, err := tx.Query(ctx,
			`SELECT id, sha256, mission_id, produced_by, schema_ref, size, created_at
			 FROM artifacts WHERE id=ANY($1) AND mission_id=$2`, sub.Grants.ArtifactRefs, sub.MissionID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var artifact store.Artifact
			var producedBy, schemaRef *string
			if err := rows.Scan(&artifact.ID, &artifact.SHA256, &artifact.MissionID, &producedBy,
				&schemaRef, &artifact.Size, &artifact.CreatedAt); err != nil {
				rows.Close()
				return nil, err
			}
			if producedBy != nil {
				artifact.ProducedBy = *producedBy
			}
			if schemaRef != nil {
				artifact.SchemaRef = *schemaRef
			}
			artifacts = append(artifacts, &artifact)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	views := map[string]*store.ContextBoardEntry{}
	for _, grant := range sub.Grants.BoardViews {
		rows, err := tx.Query(ctx,
			`SELECT namespace, key, value, version FROM board_entries
			 WHERE mission_id=$1 AND namespace=$2 AND (cardinality($3::text[])=0 OR key=ANY($3))`,
			sub.MissionID, grant.Namespace, grant.Keys)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var entry store.ContextBoardEntry
			if err := rows.Scan(&entry.Namespace, &entry.Key, &entry.Value, &entry.Version); err != nil {
				rows.Close()
				return nil, err
			}
			entry.Mode = grant.Mode
			key := entry.Namespace + "\x00" + entry.Key
			current := views[key]
			if current == nil || current.Mode == mission.BoardModeReadOnly && grant.Mode == mission.BoardModeReadWrite {
				cp := entry
				views[key] = &cp
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	board := make([]*store.ContextBoardEntry, 0, len(views))
	for _, view := range views {
		board = append(board, view)
	}
	rows, err := tx.Query(ctx, `SELECT `+decisionCols+` FROM decisions WHERE subtask_id=$1`, sub.ID)
	if err != nil {
		return nil, err
	}
	var decisions []*store.Decision
	for rows.Next() {
		decision, err := scanDecision(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		decisions = append(decisions, decision)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	var budget store.BudgetAccount
	var budgetPtr *store.BudgetAccount
	err = tx.QueryRow(ctx,
		`SELECT mission_id, total_tokens, held_tokens, spent_tokens, version, updated_at
		 FROM mission_budgets WHERE mission_id=$1`, sub.MissionID).
		Scan(&budget.MissionID, &budget.Total, &budget.Held, &budget.Spent, &budget.Version, &budget.UpdatedAt)
	if err == nil {
		budget.Metered = true
		budget.Available = budget.Total - budget.Held - budget.Spent
		budgetPtr = &budget
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	return store.BuildContextPackage(leaseID, sub, artifacts, board, decisions, budgetPtr, now)
}

// loadActiveLease 校验子任务的活跃租约与 fencing token。
func loadActiveLease(ctx context.Context, tx pgx.Tx, sub *mission.Subtask, fencingToken int64) error {
	if sub.LeaseID == "" {
		return store.ErrFenced
	}
	var token int64
	var state string
	err := tx.QueryRow(ctx,
		`SELECT fencing_token, state FROM leases WHERE id=$1 FOR UPDATE`, sub.LeaseID).Scan(&token, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrFenced
	}
	if err != nil {
		return err
	}
	if state != store.LeaseActive || token != fencingToken {
		return store.ErrFenced
	}
	return nil
}

func loadOwnedActiveLease(ctx context.Context, tx pgx.Tx, sub *mission.Subtask,
	fencingToken int64, agentID string, now time.Time) error {
	if sub.LeaseID == "" {
		return store.ErrFenced
	}
	var token int64
	var state, owner string
	var expiresAt time.Time
	err := tx.QueryRow(ctx,
		`SELECT fencing_token, state, agent_id, expires_at FROM leases WHERE id=$1 FOR UPDATE`, sub.LeaseID).
		Scan(&token, &state, &owner, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrFenced
	}
	if err != nil {
		return err
	}
	if state != store.LeaseActive || token != fencingToken || owner != agentID || !expiresAt.After(now) {
		return store.ErrFenced
	}
	return nil
}

func releaseLease(ctx context.Context, tx pgx.Tx, leaseID string) error {
	_, err := tx.Exec(ctx, `UPDATE leases SET state='RELEASED' WHERE id=$1`, leaseID)
	return err
}

// fencedTransition 执行回调类迁移的公共骨架：锁行 → 版本 → fencing → 迁移 → 事件。
func (s *Store) fencedTransition(ctx context.Context, id string, fencingToken int64, expectedVersion int64,
	evName mission.EventType, payload map[string]any, actor store.Actor, now time.Time,
	mutate func(*mission.Subtask) error) (*mission.Subtask, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	sub, err := scanSubtask(tx.QueryRow(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE id=$1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	if err := loadActiveLease(ctx, tx, sub, fencingToken); err != nil {
		return nil, err
	}
	next, err := mission.Apply(sub.State, evName)
	if err != nil {
		return nil, store.ErrConflict
	}
	sub.State = next
	if mutate != nil {
		if err := mutate(sub); err != nil {
			return nil, err
		}
	}
	if err := updateSubtask(ctx, tx, sub, expectedVersion, now); err != nil {
		return nil, err
	}
	if payload == nil {
		payload = map[string]any{}
	}
	payload["state"] = string(next)
	ev := &store.Event{AggregateID: sub.ID, MissionID: sub.MissionID, Type: string(evName),
		Payload: payload, Actor: actor, Ts: now}
	if err := appendEvent(ctx, tx, ev); err != nil {
		return nil, err
	}
	sub.Version++
	return sub, tx.Commit(ctx)
}

func (s *Store) AcceptLease(ctx context.Context, leaseID string, fencingToken int64, expectedSubVersion int64,
	actor store.Actor, now time.Time) (*mission.Subtask, error) {
	// 先按 leaseID 找子任务
	var subtaskID string
	err := s.pool.QueryRow(ctx, `SELECT subtask_id FROM leases WHERE id=$1`, leaseID).Scan(&subtaskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return s.fencedTransition(ctx, subtaskID, fencingToken, expectedSubVersion,
		mission.EvLeaseAccepted, map[string]any{"lease_id": leaseID}, actor, now, nil)
}

func (s *Store) GetLease(ctx context.Context, id string) (*store.Lease, error) {
	var l store.Lease
	err := s.pool.QueryRow(ctx,
		`SELECT id, subtask_id, agent_id, fencing_token, expires_at, state, created_at FROM leases WHERE id=$1`, id).
		Scan(&l.ID, &l.SubtaskID, &l.AgentID, &l.FencingToken, &l.ExpiresAt, &l.State, &l.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return &l, err
}

func (s *Store) GetContextPackage(ctx context.Context, leaseID string) (*store.ContextPackage, error) {
	var payload []byte
	err := s.pool.QueryRow(ctx, `SELECT payload FROM context_packages WHERE lease_id=$1`, leaseID).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var pkg store.ContextPackage
	if err := json.Unmarshal(payload, &pkg); err != nil {
		return nil, err
	}
	return &pkg, nil
}

func (s *Store) RenewLease(ctx context.Context, leaseID string, fencingToken int64, ttl time.Duration, now time.Time) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE leases SET expires_at=$1 WHERE id=$2 AND fencing_token=$3 AND state='ACTIVE'`,
		now.Add(ttl), leaseID, fencingToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrFenced
	}
	return nil
}

func (s *Store) ExpireLeases(ctx context.Context, now time.Time) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx,
		`SELECT l.id, l.subtask_id FROM leases l
		 JOIN subtasks s ON s.id=l.subtask_id
		 WHERE l.state='ACTIVE' AND l.expires_at<=$1 AND s.state IN ('OFFERED','LEASED')
		 FOR UPDATE OF l, s`, now)
	if err != nil {
		return 0, err
	}
	type expired struct{ leaseID, subtaskID string }
	var list []expired
	for rows.Next() {
		var e expired
		if err := rows.Scan(&e.leaseID, &e.subtaskID); err != nil {
			rows.Close()
			return 0, err
		}
		list = append(list, e)
	}
	rows.Close()

	n := 0
	for _, e := range list {
		if _, err := tx.Exec(ctx, `UPDATE leases SET state='EXPIRED' WHERE id=$1`, e.leaseID); err != nil {
			return n, err
		}
		// 查询已限定 OFFERED/LEASED；RUNNING Lead 由 takeover 原子 fence，不能先过期租约。
		tag, err := tx.Exec(ctx,
			`UPDATE subtasks SET state='READY', assignee_agent_id=NULL, lease_id=NULL,
			 version=version+1, updated_at=$1
			 WHERE id=$2 AND state IN ('OFFERED','LEASED')`, now, e.subtaskID)
		if err != nil {
			return n, err
		}
		if tag.RowsAffected() > 0 {
			var missionID string
			_ = tx.QueryRow(ctx, `SELECT mission_id FROM subtasks WHERE id=$1`, e.subtaskID).Scan(&missionID)
			ev := &store.Event{AggregateID: e.subtaskID, MissionID: missionID, Type: string(mission.EvLeaseExpired),
				Payload: map[string]any{"state": string(mission.StateReady), "lease_id": e.leaseID},
				Actor:   store.Actor{Kind: "system", ID: "lease-sweeper"}, Ts: now}
			if err := appendEvent(ctx, tx, ev); err != nil {
				return n, err
			}
			n++
		}
	}
	return n, tx.Commit(ctx)
}

// ---- 执行回调 ----

func (s *Store) StartSubtask(ctx context.Context, id string, fencingToken int64, expectedVersion int64,
	actor store.Actor, now time.Time) (*mission.Subtask, error) {
	return s.fencedTransition(ctx, id, fencingToken, expectedVersion, mission.EvStarted,
		map[string]any{}, actor, now, nil)
}

func (s *Store) CompleteSubtask(ctx context.Context, id string, fencingToken int64, idemKey, resultRef string,
	expectedVersion int64, actor store.Actor, now time.Time) (*mission.Subtask, error) {
	return s.CompleteSubtaskWithUsage(ctx, id, fencingToken, idemKey, resultRef, 0, expectedVersion, actor, now)
}

func (s *Store) CompleteSubtaskWithUsage(ctx context.Context, id string, fencingToken int64, idemKey, resultRef string,
	usageTokens, expectedVersion int64, actor store.Actor, now time.Time) (*mission.Subtask, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// 幂等优先：重复上报直接返回现状（§4.3）
	var existing string
	if err := tx.QueryRow(ctx, `SELECT result FROM idempotency_keys WHERE key=$1`, idemKey).
		Scan(&existing); err == nil {
		sub, gerr := scanSubtask(tx.QueryRow(ctx,
			`SELECT `+subtaskCols+` FROM subtasks WHERE id=$1`, id))
		if gerr != nil {
			return nil, gerr
		}
		return sub, store.ErrDuplicate
	}

	sub, err := scanSubtask(tx.QueryRow(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE id=$1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	if err := loadActiveLease(ctx, tx, sub, fencingToken); err != nil {
		return nil, err
	}
	if _, err := mission.Apply(sub.State, mission.EvCompleted); err != nil {
		return nil, store.ErrConflict
	}
	if err := settleBudgetHold(ctx, tx, sub, usageTokens, actor, now); err != nil {
		return nil, err
	}
	sub.State = mission.StateSucceeded
	sub.ResultRef = resultRef
	if err := updateSubtask(ctx, tx, sub, expectedVersion, now); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO idempotency_keys (key, result, created_at) VALUES ($1,$2,$3)`,
		idemKey, resultRef, now); err != nil {
		return nil, err
	}
	if err := recordLeaseMeter(ctx, tx, sub, actor.ID, now); err != nil {
		return nil, err
	}
	if usageTokens > 0 {
		if err := putMeterTx(ctx, tx, &store.MeterRecord{ID: "meter:token:" + sub.ID + ":" + idemKey,
			MissionID: sub.MissionID, SubtaskID: sub.ID, AgentID: actor.ID,
			Resource: "token.reported", Quantity: float64(usageTokens), Trust: store.MeterSelfReported}, now); err != nil {
			return nil, err
		}
	}
	if err := releaseLease(ctx, tx, sub.LeaseID); err != nil {
		return nil, err
	}
	ev := &store.Event{AggregateID: sub.ID, MissionID: sub.MissionID, Type: string(mission.EvCompleted),
		Payload: map[string]any{"state": string(mission.StateSucceeded),
			"subtask_id": sub.ID, // M6：Lead event 唤醒以 where 谓词精确等待特定子女
			"result_ref": resultRef},
		Actor: actor, Ts: now}
	if err := appendEvent(ctx, tx, ev); err != nil {
		return nil, err
	}
	if sub.ParentID != "" {
		itemID := store.LeadInboxID(sub.ID)
		tag, err := tx.Exec(ctx,
			`INSERT INTO lead_inbox
			 (id, mission_id, lead_subtask_id, source_subtask_id, kind, result_ref, status, created_at)
			 VALUES ($1,$2,$3,$4,'result',$5,$6,$7)
			 ON CONFLICT (source_subtask_id) DO NOTHING`,
			itemID, sub.MissionID, sub.ParentID, sub.ID, resultRef, store.LeadInboxPending, now)
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() > 0 {
			if err := appendEvent(ctx, tx, &store.Event{AggregateID: sub.ParentID, MissionID: sub.MissionID,
				Type: "lead.inbox.enqueued", Payload: map[string]any{
					"item_id": itemID, "source_subtask_id": sub.ID, "result_ref": resultRef,
				}, Actor: store.Actor{Kind: "system", ID: "lead-inbox"}, Ts: now}); err != nil {
				return nil, err
			}
		}
	}
	sub.Version++
	return sub, tx.Commit(ctx)
}

func settleBudgetHold(ctx context.Context, tx pgx.Tx, sub *mission.Subtask, usageTokens int64,
	actor store.Actor, now time.Time) error {
	var hold store.BudgetHold
	err := tx.QueryRow(ctx,
		`SELECT id, mission_id, subtask_id, attempt, amount_tokens, actual_tokens,
		 status, created_at, settled_at
		 FROM budget_holds WHERE subtask_id=$1 FOR UPDATE`, sub.ID).
		Scan(&hold.ID, &hold.MissionID, &hold.SubtaskID, &hold.Attempt, &hold.Amount,
			&hold.Actual, &hold.Status, &hold.CreatedAt, &hold.SettledAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if hold.Status != store.BudgetHoldHeld {
		return nil
	}
	if usageTokens < 0 {
		return store.ErrBudgetExceeded
	}
	var total, held, spent int64
	if err := tx.QueryRow(ctx,
		`SELECT total_tokens, held_tokens, spent_tokens
		 FROM mission_budgets WHERE mission_id=$1 FOR UPDATE`, sub.MissionID).
		Scan(&total, &held, &spent); err != nil {
		return err
	}
	otherHeld := held - hold.Amount
	if otherHeld < 0 || usageTokens > total-spent-otherHeld {
		return store.ErrBudgetExceeded
	}
	if _, err := tx.Exec(ctx,
		`UPDATE mission_budgets SET held_tokens=$2, spent_tokens=$3, version=version+1, updated_at=$4
		 WHERE mission_id=$1`, sub.MissionID, otherHeld, spent+usageTokens, now); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE budget_holds SET actual_tokens=$2, status=$3, settled_at=$4 WHERE id=$1`,
		hold.ID, usageTokens, store.BudgetHoldSettled, now); err != nil {
		return err
	}
	return appendEvent(ctx, tx, &store.Event{AggregateID: sub.ID, MissionID: sub.MissionID,
		Type: "budget.settled", Payload: map[string]any{
			"hold_id": hold.ID, "reserved_tokens": hold.Amount, "actual_tokens": usageTokens,
			"available_tokens": total - otherHeld - spent - usageTokens,
		}, Actor: actor, Ts: now})
}

func releaseBudgetHold(ctx context.Context, tx pgx.Tx, sub *mission.Subtask, reason string,
	actor store.Actor, now time.Time) error {
	var holdID string
	var amount int64
	err := tx.QueryRow(ctx,
		`SELECT id, amount_tokens FROM budget_holds
		 WHERE subtask_id=$1 AND status=$2 FOR UPDATE`, sub.ID, store.BudgetHoldHeld).
		Scan(&holdID, &amount)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var total, held, spent int64
	if err := tx.QueryRow(ctx,
		`SELECT total_tokens, held_tokens, spent_tokens FROM mission_budgets
		 WHERE mission_id=$1 FOR UPDATE`, sub.MissionID).Scan(&total, &held, &spent); err != nil {
		return err
	}
	if held < amount {
		return store.ErrConflict
	}
	if _, err := tx.Exec(ctx,
		`UPDATE mission_budgets SET held_tokens=held_tokens-$2, version=version+1, updated_at=$3
		 WHERE mission_id=$1`, sub.MissionID, amount, now); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE budget_holds SET status=$2, settled_at=$3 WHERE id=$1`,
		holdID, store.BudgetHoldReleased, now); err != nil {
		return err
	}
	return appendEvent(ctx, tx, &store.Event{AggregateID: sub.ID, MissionID: sub.MissionID,
		Type: "budget.released", Payload: map[string]any{
			"hold_id": holdID, "amount_tokens": amount, "reason": reason,
			"available_tokens": total - (held - amount) - spent,
		}, Actor: actor, Ts: now})
}

// ---- 主子委托（M6，§15.1） ----

// SpawnSubtask 原子完成：幂等去重 → 父任务 fencing + RUNNING 校验 → 子女插入并激活 READY。
func (s *Store) SpawnSubtask(ctx context.Context, idemKey, parentID string, fencingToken, parentVersion int64,
	child *mission.Subtask, actor store.Actor, now time.Time) (string, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)

	// 幂等优先：重复委托直接返回原子女（恰好一次，§4.3）
	var existing string
	if err := tx.QueryRow(ctx, `SELECT result FROM idempotency_keys WHERE key=$1`, idemKey).
		Scan(&existing); err == nil {
		return existing, store.ErrDuplicate
	}
	parent, err := scanSubtask(tx.QueryRow(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE id=$1 FOR UPDATE`, parentID))
	if errors.Is(err, pgx.ErrNoRows) {
		return "", store.ErrNotFound
	}
	if err != nil {
		return "", err
	}
	if parent.Version != parentVersion {
		return "", store.ErrConflict
	}
	if err := loadOwnedActiveLease(ctx, tx, parent, fencingToken, actor.ID, now); err != nil {
		return "", err
	}
	if parent.State != mission.StateRunning {
		return "", store.ErrConflict // 只有 RUNNING 中的 Lead 能 delegate（§15.1 时序约束）
	}
	if !mission.PermissionEnvelopeSubset(parent.Grants, child.Grants) {
		return "", store.ErrPermissionExceeded
	}
	var total, held, spent int64
	budgetErr := tx.QueryRow(ctx,
		`SELECT total_tokens, held_tokens, spent_tokens FROM mission_budgets
		 WHERE mission_id=$1 FOR UPDATE`, child.MissionID).Scan(&total, &held, &spent)
	metered := budgetErr == nil
	if budgetErr != nil && !errors.Is(budgetErr, pgx.ErrNoRows) {
		return "", budgetErr
	}
	if metered {
		amount := child.Scheduling.BudgetTokens
		if amount <= 0 {
			return "", store.ErrBudgetRequired
		}
		if total-held-spent < amount {
			return "", store.ErrBudgetExceeded
		}
	}
	child.State = mission.StateReady
	child.Version = 1
	spec := marshalSpec(child)
	scheduling, _ := js(child.Scheduling)
	retry, _ := js(child.Retry)
	if _, err := tx.Exec(ctx,
		`INSERT INTO subtasks (id, mission_id, parent_id, kind, spec, scheduling, retry, state,
		 depends_on, attempt, version, created_at, updated_at)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'{}',0,1,$9,$9)`,
		child.ID, child.MissionID, parentID, string(child.Kind), spec, scheduling, retry,
		string(child.State), now); err != nil {
		return "", store.ErrConflict // 子女 ID 冲突
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO idempotency_keys (key, result, created_at) VALUES ($1,$2,$3)`,
		idemKey, child.ID, now); err != nil {
		return "", err
	}
	if metered {
		amount := child.Scheduling.BudgetTokens
		holdID := store.BudgetHoldID(child.ID)
		if _, err := tx.Exec(ctx,
			`INSERT INTO budget_holds
			 (id, mission_id, subtask_id, attempt, amount_tokens, actual_tokens, status, created_at)
			 VALUES ($1,$2,$3,$4,$5,0,$6,$7)`, holdID, child.MissionID, child.ID,
			child.Attempt, amount, store.BudgetHoldHeld, now); err != nil {
			return "", err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE mission_budgets SET held_tokens=held_tokens+$2, version=version+1, updated_at=$3
			 WHERE mission_id=$1`, child.MissionID, amount, now); err != nil {
			return "", err
		}
		if err := appendEvent(ctx, tx, &store.Event{AggregateID: child.ID, MissionID: child.MissionID,
			Type: "budget.held", Payload: map[string]any{
				"hold_id": holdID, "amount_tokens": amount,
				"available_tokens": total - held - spent - amount,
			}, Actor: actor, Ts: now}); err != nil {
			return "", err
		}
	}
	ev := &store.Event{AggregateID: child.ID, MissionID: child.MissionID, Type: string(mission.EvCreated),
		Payload: map[string]any{
			"kind":              string(child.Kind),
			"parent_subtask_id": parentID,
			"rework_of":         child.ReworkOf,
		}, Actor: actor, Ts: now}
	if err := appendEvent(ctx, tx, ev); err != nil {
		return "", err
	}
	if err := appendEvent(ctx, tx, &store.Event{AggregateID: child.ID, MissionID: child.MissionID,
		Type: string(mission.EvDepsSatisfied), Payload: map[string]any{"state": string(mission.StateReady)},
		Actor: store.Actor{Kind: "system", ID: "orchestrator"}, Ts: now}); err != nil {
		return "", err
	}
	return "", tx.Commit(ctx)
}

// CountChildren 直接子女计数（delegate fanout 校验）。
func (s *Store) CountChildren(ctx context.Context, parentID string) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM subtasks WHERE parent_id=$1`, parentID).Scan(&n)
	return n, err
}

func scanLeadInbox(row pgx.Row) (*store.LeadInboxItem, error) {
	var item store.LeadInboxItem
	var ingestedBy *string
	err := row.Scan(&item.ID, &item.MissionID, &item.LeadSubtaskID, &item.SourceSubtaskID,
		&item.Kind, &item.ResultRef, &item.Status, &item.IngestMode, &ingestedBy,
		&item.Version, &item.CreatedAt, &item.IngestedAt)
	if ingestedBy != nil {
		item.IngestedBy = *ingestedBy
	}
	return &item, err
}

const leadInboxCols = `id, mission_id, lead_subtask_id, source_subtask_id, kind, result_ref,
	status, ingest_mode, ingested_by, version, created_at, ingested_at`

func (s *Store) ListLeadInbox(ctx context.Context, leadSubtaskID string, pendingOnly bool) ([]*store.LeadInboxItem, error) {
	q := `SELECT ` + leadInboxCols + ` FROM lead_inbox WHERE lead_subtask_id=$1`
	if pendingOnly {
		q += ` AND status='pending'`
	}
	q += ` ORDER BY created_at, id`
	rows, err := s.pool.Query(ctx, q, leadSubtaskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.LeadInboxItem
	for rows.Next() {
		item, err := scanLeadInbox(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) IngestLeadInbox(ctx context.Context, itemID, leadSubtaskID string,
	fencingToken, expectedVersion int64, mode string, actor store.Actor, now time.Time) (*store.LeadInboxItem, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	item, err := scanLeadInbox(tx.QueryRow(ctx,
		`SELECT `+leadInboxCols+` FROM lead_inbox WHERE id=$1 FOR UPDATE`, itemID))
	if errors.Is(err, pgx.ErrNoRows) || err == nil && item.LeadSubtaskID != leadSubtaskID {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	lead, err := scanSubtask(tx.QueryRow(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE id=$1 FOR UPDATE`, leadSubtaskID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if lead.State != mission.StateRunning {
		return nil, store.ErrConflict
	}
	if err := loadOwnedActiveLease(ctx, tx, lead, fencingToken, actor.ID, now); err != nil {
		return nil, err
	}
	if item.Status != store.LeadInboxPending || item.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	tag, err := tx.Exec(ctx,
		`UPDATE lead_inbox SET status=$1, ingest_mode=$2, ingested_by=$3, ingested_at=$4,
		 version=version+1 WHERE id=$5 AND status='pending' AND version=$6`,
		store.LeadInboxIngested, mode, actor.ID, now, item.ID, expectedVersion)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, store.ErrConflict
	}
	if err := appendEvent(ctx, tx, &store.Event{AggregateID: lead.ID, MissionID: lead.MissionID,
		Type: "lead.inbox.ingested", Payload: map[string]any{
			"item_id": item.ID, "source_subtask_id": item.SourceSubtaskID, "mode": mode,
		}, Actor: actor, Ts: now}); err != nil {
		return nil, err
	}
	item.Status, item.IngestMode, item.IngestedBy = store.LeadInboxIngested, mode, actor.ID
	item.IngestedAt = &now
	item.Version++
	return item, tx.Commit(ctx)
}

func (s *Store) SaveLeadSnapshot(ctx context.Context, leadSubtaskID string,
	fencingToken, expectedVersion int64, value []byte, leaseTTL time.Duration,
	actor store.Actor, now time.Time) (*store.BoardEntry, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	lead, err := scanSubtask(tx.QueryRow(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE id=$1 FOR UPDATE`, leadSubtaskID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if lead.State != mission.StateRunning {
		return nil, store.ErrConflict
	}
	if err := loadOwnedActiveLease(ctx, tx, lead, fencingToken, actor.ID, now); err != nil {
		return nil, err
	}
	val, err := js(json.RawMessage(value))
	if err != nil {
		return nil, err
	}
	entry := &store.BoardEntry{MissionID: lead.MissionID, Namespace: "lead-plan", Key: lead.ID,
		Value: append([]byte(nil), value...), UpdatedAt: now}
	if expectedVersion == -1 {
		err = tx.QueryRow(ctx,
			`INSERT INTO board_entries (mission_id, namespace, key, value, version, updated_at)
			 VALUES ($1,'lead-plan',$2,$3,0,$4)
			 ON CONFLICT (mission_id, namespace, key) DO NOTHING RETURNING version`,
			lead.MissionID, lead.ID, val, now).Scan(&entry.Version)
	} else {
		err = tx.QueryRow(ctx,
			`UPDATE board_entries SET value=$1, version=version+1, updated_at=$2
			 WHERE mission_id=$3 AND namespace='lead-plan' AND key=$4 AND version=$5
			 RETURNING version`, val, now, lead.MissionID, lead.ID, expectedVersion).Scan(&entry.Version)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrConflict
	}
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE leases SET expires_at=$1 WHERE id=$2`,
		now.Add(leaseTTL), lead.LeaseID); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE agents SET health=jsonb_set(
		   jsonb_set(health,'{last_heartbeat}',to_jsonb($2::timestamptz)),
		   '{status}',to_jsonb(CASE WHEN health->>'status'='suspect' THEN 'healthy'
		                           ELSE health->>'status' END)),
		 updated_at=$2 WHERE id=$1`, actor.ID, now); err != nil {
		return nil, err
	}
	if err := appendEvent(ctx, tx, &store.Event{AggregateID: lead.ID, MissionID: lead.MissionID,
		Type: "lead.snapshot.saved", Payload: map[string]any{"version": entry.Version},
		Actor: actor, Ts: now}); err != nil {
		return nil, err
	}
	return entry, tx.Commit(ctx)
}

func (s *Store) TakeoverStaleLeads(ctx context.Context, now time.Time) ([]*mission.Subtask, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx,
		`SELECT s.id FROM subtasks s JOIN leases l ON l.id=s.lease_id
		 WHERE s.state='RUNNING' AND l.state='ACTIVE' AND l.expires_at<=$1
		 AND (EXISTS (SELECT 1 FROM subtasks c WHERE c.parent_id=s.id)
		      OR EXISTS (SELECT 1 FROM board_entries b
		                 WHERE b.mission_id=s.mission_id AND b.namespace='lead-plan' AND b.key=s.id))
		 ORDER BY s.id FOR UPDATE OF s, l`, now)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	var out []*mission.Subtask
	for _, id := range ids {
		lead, err := scanSubtask(tx.QueryRow(ctx,
			`SELECT `+subtaskCols+` FROM subtasks WHERE id=$1`, id))
		if err != nil {
			return nil, err
		}
		if _, err := mission.Apply(lead.State, mission.EvTakeover); err != nil {
			return nil, store.ErrConflict
		}
		oldAgent, oldLease := lead.Assignee, lead.LeaseID
		if _, err := tx.Exec(ctx, `UPDATE leases SET state='FENCED' WHERE id=$1 AND state='ACTIVE'`, oldLease); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx,
			`UPDATE agents SET health=jsonb_set(health,'{status}',to_jsonb(
			 CASE WHEN health->>'status'='down' THEN 'down' ELSE 'suspect' END)),
			 updated_at=$1 WHERE id=$2`, now, oldAgent); err != nil {
			return nil, err
		}
		lead.State, lead.Assignee, lead.LeaseID = mission.StateReady, "", ""
		if err := updateSubtask(ctx, tx, lead, lead.Version, now); err != nil {
			return nil, err
		}
		if err := appendEvent(ctx, tx, &store.Event{AggregateID: lead.ID, MissionID: lead.MissionID,
			Type: string(mission.EvTakeover), Payload: map[string]any{
				"state": string(mission.StateReady), "fenced_lease_id": oldLease, "previous_agent_id": oldAgent,
			}, Actor: store.Actor{Kind: "system", ID: "lead-takeover"}, Ts: now}); err != nil {
			return nil, err
		}
		lead.Version++
		out = append(out, lead)
	}
	return out, tx.Commit(ctx)
}

func (s *Store) FailSubtask(ctx context.Context, id string, fencingToken int64, reason string,
	expectedVersion int64, actor store.Actor, now time.Time) (*mission.Subtask, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	sub, err := scanSubtask(tx.QueryRow(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE id=$1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	if err := loadActiveLease(ctx, tx, sub, fencingToken); err != nil {
		return nil, err
	}
	if _, err := mission.Apply(sub.State, mission.EvFailed); err != nil {
		return nil, store.ErrConflict
	}
	leaseID := sub.LeaseID
	sub.State = mission.StateFailed
	if err := updateSubtask(ctx, tx, sub, expectedVersion, now); err != nil {
		return nil, err
	}
	if err := recordLeaseMeter(ctx, tx, sub, actor.ID, now); err != nil {
		return nil, err
	}
	if err := releaseLease(ctx, tx, leaseID); err != nil {
		return nil, err
	}
	if sub.Attempt >= sub.Retry.MaxAttempts {
		if err := releaseBudgetHold(ctx, tx, sub, "final_failure", actor, now); err != nil {
			return nil, err
		}
	}
	if err := appendEvent(ctx, tx, &store.Event{AggregateID: sub.ID, MissionID: sub.MissionID,
		Type: string(mission.EvFailed), Payload: map[string]any{
			"state": string(mission.StateFailed), "reason": reason}, Actor: actor, Ts: now}); err != nil {
		return nil, err
	}
	sub.Version++
	return sub, tx.Commit(ctx)
}

func (s *Store) BlockSubtask(ctx context.Context, id string, fencingToken int64, expectedVersion int64,
	actor store.Actor, now time.Time) (*mission.Subtask, error) {
	return s.fencedTransition(ctx, id, fencingToken, expectedVersion, mission.EvBlocked,
		map[string]any{}, actor, now, nil)
}

func (s *Store) CancelSubtask(ctx context.Context, id string, expectedVersion int64,
	actor store.Actor, now time.Time) (*mission.Subtask, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	sub, err := scanSubtask(tx.QueryRow(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE id=$1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	if _, err := mission.Apply(sub.State, mission.EvCancelled); err != nil {
		return nil, store.ErrConflict
	}
	leaseID := sub.LeaseID
	sub.State = mission.StateCancelled
	sub.Assignee, sub.LeaseID = "", ""
	if err := updateSubtask(ctx, tx, sub, expectedVersion, now); err != nil {
		return nil, err
	}
	if leaseID != "" {
		if err := releaseLease(ctx, tx, leaseID); err != nil {
			return nil, err
		}
	}
	if err := releaseBudgetHold(ctx, tx, sub, "cancelled", actor, now); err != nil {
		return nil, err
	}
	if err := appendEvent(ctx, tx, &store.Event{AggregateID: sub.ID, MissionID: sub.MissionID,
		Type: string(mission.EvCancelled), Payload: map[string]any{"state": string(mission.StateCancelled)},
		Actor: actor, Ts: now}); err != nil {
		return nil, err
	}
	sub.Version++
	return sub, tx.Commit(ctx)
}

// ---- 挂起-唤醒（M3） ----

func (s *Store) SuspendSubtask(ctx context.Context, id string, fencingToken int64, expectedVersion int64,
	wake *mission.WakeSpec, checkpoint []byte,
	actor store.Actor, now time.Time) (*mission.Subtask, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	sub, err := scanSubtask(tx.QueryRow(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE id=$1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	if err := loadActiveLease(ctx, tx, sub, fencingToken); err != nil {
		return nil, err
	}
	if _, err := mission.Apply(sub.State, mission.EvSuspended); err != nil {
		return nil, store.ErrConflict
	}
	spec, err := json.Marshal(wake)
	if err != nil {
		return nil, err
	}
	leaseID := sub.LeaseID
	sub.State = mission.StateWaiting
	sub.WakeKind, sub.WakeAt, sub.WakeDeadline = wake.Kind, wake.At, wake.Deadline
	sub.WakeSpec = spec
	if len(checkpoint) > 0 {
		sub.Checkpoint = checkpoint
	}
	sub.Assignee, sub.LeaseID = "", ""
	if err := updateSubtask(ctx, tx, sub, expectedVersion, now); err != nil {
		return nil, err
	}
	if err := releaseLease(ctx, tx, leaseID); err != nil {
		return nil, err
	}
	if err := appendEvent(ctx, tx, &store.Event{AggregateID: sub.ID, MissionID: sub.MissionID,
		Type:    string(mission.EvSuspended),
		Payload: map[string]any{"state": string(mission.StateWaiting), "wake_kind": wake.Kind},
		Actor:   actor, Ts: now}); err != nil {
		return nil, err
	}
	sub.Version++
	return sub, tx.Commit(ctx)
}

func (s *Store) WakeSubtask(ctx context.Context, id string, expectedVersion int64,
	actor store.Actor, now time.Time) (*mission.Subtask, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	sub, err := scanSubtask(tx.QueryRow(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE id=$1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict // CAS 竞争：另一 sweeper/wake 已处理
	}
	if sub.WakeDeadline != nil && !sub.WakeDeadline.After(now) {
		return nil, store.ErrConflict
	}
	if _, err := mission.Apply(sub.State, mission.EvWoken); err != nil {
		return nil, store.ErrConflict
	}
	sub.State = mission.StateReady
	sub.WakeKind, sub.WakeAt, sub.WakeDeadline, sub.WakeSpec = "", nil, nil, nil // 一次性注册，清空
	if err := updateSubtask(ctx, tx, sub, expectedVersion, now); err != nil {
		return nil, err
	}
	if err := putMeterTx(ctx, tx, &store.MeterRecord{ID: fmt.Sprintf("meter:wake:%s:%d", sub.ID, expectedVersion+1),
		MissionID: sub.MissionID, SubtaskID: sub.ID, Resource: "wake.fire", Quantity: 1,
		Trust: store.MeterAuthoritative}, now); err != nil {
		return nil, err
	}
	if err := appendEvent(ctx, tx, &store.Event{AggregateID: sub.ID, MissionID: sub.MissionID,
		Type: string(mission.EvWoken), Payload: map[string]any{"state": string(mission.StateReady)},
		Actor: actor, Ts: now}); err != nil {
		return nil, err
	}
	sub.Version++
	return sub, tx.Commit(ctx)
}

func (s *Store) ListWaitingDue(ctx context.Context, now time.Time) ([]*mission.Subtask, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+subtaskCols+` FROM subtasks
		 WHERE state='WAITING' AND wake_kind='timer' AND wake_at<=$1
		   AND (wake_deadline IS NULL OR wake_deadline>$1) ORDER BY wake_at`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*mission.Subtask
	for rows.Next() {
		sub, err := scanSubtask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

func (s *Store) ExpireWakes(ctx context.Context, now time.Time) ([]*mission.Subtask, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx,
		`SELECT `+subtaskCols+` FROM subtasks
		 WHERE state='WAITING' AND wake_deadline IS NOT NULL AND wake_deadline<=$1
		 ORDER BY id FOR UPDATE`, now)
	if err != nil {
		return nil, err
	}
	var expired []*mission.Subtask
	for rows.Next() {
		sub, err := scanSubtask(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		expired = append(expired, sub)
	}
	rows.Close()

	for _, sub := range expired {
		sub.State = mission.StateFailed
		sub.WakeKind, sub.WakeAt, sub.WakeDeadline, sub.WakeSpec = "", nil, nil, nil
		if err := updateSubtask(ctx, tx, sub, sub.Version, now); err != nil {
			return nil, err
		}
		sub.Version++
		if err := appendEvent(ctx, tx, &store.Event{AggregateID: sub.ID, MissionID: sub.MissionID,
			Type:    string(mission.EvFailed),
			Payload: map[string]any{"state": string(mission.StateFailed), "reason": "wake_timeout"},
			Actor:   store.Actor{Kind: "system", ID: "sweeper"}, Ts: now}); err != nil {
			return nil, err
		}
	}
	return expired, tx.Commit(ctx)
}

func (s *Store) SaveCheckpoint(ctx context.Context, id string, fencingToken int64, checkpoint []byte, _ time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	sub, err := scanSubtask(tx.QueryRow(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE id=$1 FOR UPDATE`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return store.ErrNotFound
	}
	if err != nil {
		return err
	}
	if err := loadActiveLease(ctx, tx, sub, fencingToken); err != nil {
		return err
	}
	if sub.State != mission.StateRunning && sub.State != mission.StateBlocked {
		return store.ErrConflict
	}
	if _, err := tx.Exec(ctx, `UPDATE subtasks SET checkpoint=$1 WHERE id=$2`, checkpoint, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) ListWaiting(ctx context.Context, wakeKind string) ([]*mission.Subtask, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE state='WAITING' AND wake_kind=$1 ORDER BY id`, wakeKind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*mission.Subtask
	for rows.Next() {
		sub, err := scanSubtask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

func (s *Store) MaxEventSeq(ctx context.Context) (int64, error) {
	var seq int64
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(MAX(seq),0) FROM events`).Scan(&seq)
	return seq, err
}

func (s *Store) PutIdempotent(ctx context.Context, key, result string, now time.Time) (string, error) {
	var existing string
	err := s.pool.QueryRow(ctx,
		`INSERT INTO idempotency_keys (key, result, created_at) VALUES ($1,$2,$3)
		 ON CONFLICT (key) DO NOTHING RETURNING result`, key, result, now).Scan(&existing)
	if errors.Is(err, pgx.ErrNoRows) {
		// 冲突：读出既有值
		if qerr := s.pool.QueryRow(ctx, `SELECT result FROM idempotency_keys WHERE key=$1`, key).
			Scan(&existing); qerr != nil {
			return "", qerr
		}
		return existing, store.ErrDuplicate
	}
	return "", err
}

func (s *Store) GetSubtask(ctx context.Context, id string) (*mission.Subtask, error) {
	sub, err := scanSubtask(s.pool.QueryRow(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return sub, err
}

// ---- 决策（M2） ----

func scanDecision(row pgx.Row) (*store.Decision, error) {
	var d store.Decision
	var options []byte
	var choice, rationale, deciderID *string
	err := row.Scan(&d.ID, &d.MissionID, &d.SubtaskID, &d.Kind, &d.Question, &options,
		&d.Status, &choice, &rationale, &deciderID, &d.Deadline, &d.OnTimeout,
		&d.CreatedAt, &d.ResolvedAt)
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal(options, &d.Options)
	if choice != nil {
		d.Choice = *choice
	}
	if rationale != nil {
		d.Rationale = *rationale
	}
	if deciderID != nil {
		d.DeciderID = *deciderID
	}
	return &d, nil
}

const decisionCols = `id, mission_id, subtask_id, kind, question, options, status,
	choice, rationale, decider_id, deadline, on_timeout, ts, resolved_at`

func (s *Store) CreateDecision(ctx context.Context, d *store.Decision, now time.Time) error {
	options, _ := js(d.Options)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx,
		`INSERT INTO decisions (id, mission_id, subtask_id, kind, question, options, status,
		 decider_type, decider_id, deadline, on_timeout, ts)
		 VALUES ($1,$2,$3,$4,$5,$6,'pending','human',NULL,$7,$8,$9)`,
		d.ID, d.MissionID, d.SubtaskID, d.Kind, d.Question, options, d.Deadline, d.OnTimeout, now); err != nil {
		return store.ErrConflict
	}
	ev := &store.Event{AggregateID: d.SubtaskID, MissionID: d.MissionID, Type: "decision.requested",
		Payload: map[string]any{"decision_id": d.ID, "question": d.Question, "options": d.Options},
		Actor:   store.Actor{Kind: "system", ID: "hitl"}, Ts: now}
	if err := appendEvent(ctx, tx, ev); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) CreateDecisionAndBlock(ctx context.Context, d *store.Decision, expectedSubVersion int64,
	fencingToken *int64, actor store.Actor, now time.Time) (*mission.Subtask, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	sub, err := scanSubtask(tx.QueryRow(ctx,
		`SELECT `+subtaskCols+` FROM subtasks WHERE id=$1 FOR UPDATE`, d.SubtaskID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if sub.Version != expectedSubVersion {
		return nil, store.ErrConflict
	}
	if fencingToken != nil {
		if err := loadActiveLease(ctx, tx, sub, *fencingToken); err != nil {
			return nil, err
		}
	}
	if _, err := mission.Apply(sub.State, mission.EvBlocked); err != nil {
		return nil, store.ErrConflict
	}
	sub.State = mission.StateBlocked
	if err := updateSubtask(ctx, tx, sub, expectedSubVersion, now); err != nil {
		return nil, err
	}
	options, err := js(d.Options)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO decisions (id, mission_id, subtask_id, kind, question, options, status,
		 decider_type, decider_id, deadline, on_timeout, ts)
		 VALUES ($1,$2,$3,$4,$5,$6,'pending','human',NULL,$7,$8,$9)`,
		d.ID, d.MissionID, d.SubtaskID, d.Kind, d.Question, options, d.Deadline, d.OnTimeout, now); err != nil {
		return nil, store.ErrConflict
	}
	if err := appendEvent(ctx, tx, &store.Event{AggregateID: sub.ID, MissionID: sub.MissionID,
		Type: string(mission.EvBlocked), Payload: map[string]any{
			"state": string(mission.StateBlocked), "question": d.Question}, Actor: actor, Ts: now}); err != nil {
		return nil, err
	}
	if err := appendEvent(ctx, tx, &store.Event{AggregateID: d.SubtaskID, MissionID: d.MissionID,
		Type: "decision.requested", Payload: map[string]any{
			"decision_id": d.ID, "question": d.Question, "options": d.Options}, Actor: actor, Ts: now}); err != nil {
		return nil, err
	}
	d.Status, d.CreatedAt = store.DecisionPending, now
	sub.Version++
	return sub, tx.Commit(ctx)
}

func (s *Store) GetDecision(ctx context.Context, id string) (*store.Decision, error) {
	d, err := scanDecision(s.pool.QueryRow(ctx,
		`SELECT `+decisionCols+` FROM decisions WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return d, err
}

func (s *Store) ListDecisions(ctx context.Context, missionID string, pendingOnly bool) ([]*store.Decision, error) {
	q := `SELECT ` + decisionCols + ` FROM decisions WHERE ($1='' OR mission_id=$1)`
	if pendingOnly {
		q += ` AND status='pending'`
	}
	q += ` ORDER BY ts`
	rows, err := s.pool.Query(ctx, q, missionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.Decision
	for rows.Next() {
		d, err := scanDecision(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) ResolveDecision(ctx context.Context, id, choice, rationale, deciderID string, now time.Time) (*store.Decision, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx,
		`UPDATE decisions SET status='resolved', choice=$1, rationale=$2, decider_id=$3, resolved_at=$4
		 WHERE id=$5 AND status='pending'`, choice, rationale, deciderID, now, id)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, store.ErrConflict
	}
	d, err := scanDecision(tx.QueryRow(ctx, `SELECT `+decisionCols+` FROM decisions WHERE id=$1`, id))
	if err != nil {
		return nil, err
	}
	ev := &store.Event{AggregateID: d.SubtaskID, MissionID: d.MissionID, Type: "decision.resolved",
		Payload: map[string]any{"decision_id": d.ID, "choice": choice, "decider": deciderID},
		Actor:   store.Actor{Kind: "human", ID: deciderID}, Ts: now}
	if err := appendEvent(ctx, tx, ev); err != nil {
		return nil, err
	}
	return d, tx.Commit(ctx)
}

func (s *Store) ExpireDecisions(ctx context.Context, now time.Time) ([]*store.Decision, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx,
		`SELECT `+decisionCols+` FROM decisions
		 WHERE status='pending' AND deadline IS NOT NULL AND deadline<=$1 FOR UPDATE`, now)
	if err != nil {
		return nil, err
	}
	var list []*store.Decision
	for rows.Next() {
		d, err := scanDecision(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		list = append(list, d)
	}
	rows.Close()
	for _, d := range list {
		switch d.OnTimeout {
		case "auto_approve", "auto_reject":
			choice := "approve"
			if d.OnTimeout == "auto_reject" {
				choice = "reject"
			}
			if _, err := tx.Exec(ctx,
				`UPDATE decisions SET status='resolved', choice=$1, decider_id='system:timeout', resolved_at=$2
				 WHERE id=$3`, choice, now, d.ID); err != nil {
				return nil, err
			}
			d.Status, d.Choice = store.DecisionResolved, choice
		default:
			if _, err := tx.Exec(ctx,
				`UPDATE decisions SET status='expired' WHERE id=$1`, d.ID); err != nil {
				return nil, err
			}
			d.Status = store.DecisionExpired
		}
		ev := &store.Event{AggregateID: d.SubtaskID, MissionID: d.MissionID, Type: "decision.expired",
			Payload: map[string]any{"decision_id": d.ID, "on_timeout": d.OnTimeout, "choice": d.Choice},
			Actor:   store.Actor{Kind: "system", ID: "decision-sweeper"}, Ts: now}
		if err := appendEvent(ctx, tx, ev); err != nil {
			return nil, err
		}
	}
	return list, tx.Commit(ctx)
}

// ---- 黑板（M2） ----

func (s *Store) BoardPut(ctx context.Context, e *store.BoardEntry, expectedVersion int64, now time.Time) (*store.BoardEntry, error) {
	val, err := js(json.RawMessage(e.Value))
	if err != nil {
		return nil, err
	}
	if expectedVersion >= 0 {
		// CAS 路径：仅当现版本匹配才写入
		tag, err := s.pool.Exec(ctx,
			`UPDATE board_entries SET value=$1, version=version+1, updated_at=$2
			 WHERE mission_id=$3 AND namespace=$4 AND key=$5 AND version=$6`,
			val, now, e.MissionID, e.Namespace, e.Key, expectedVersion)
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() == 0 {
			return nil, store.ErrConflict
		}
		return s.BoardGet(ctx, e.MissionID, e.Namespace, e.Key)
	}
	// 盲写（upsert）
	if _, err := s.pool.Exec(ctx,
		`INSERT INTO board_entries (mission_id, namespace, key, value, version, updated_at)
		 VALUES ($1,$2,$3,$4,0,$5)
		 ON CONFLICT (mission_id, namespace, key)
		 DO UPDATE SET value=$4, version=board_entries.version+1, updated_at=$5`,
		e.MissionID, e.Namespace, e.Key, val, now); err != nil {
		return nil, err
	}
	return s.BoardGet(ctx, e.MissionID, e.Namespace, e.Key)
}

func (s *Store) BoardGet(ctx context.Context, missionID, ns, key string) (*store.BoardEntry, error) {
	var e store.BoardEntry
	var val []byte
	err := s.pool.QueryRow(ctx,
		`SELECT mission_id, namespace, key, value, version, updated_at FROM board_entries
		 WHERE mission_id=$1 AND namespace=$2 AND key=$3`, missionID, ns, key).
		Scan(&e.MissionID, &e.Namespace, &e.Key, &val, &e.Version, &e.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	e.Value = val
	return &e, err
}

func (s *Store) BoardList(ctx context.Context, missionID, ns string) ([]*store.BoardEntry, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT mission_id, namespace, key, value, version, updated_at FROM board_entries
		 WHERE mission_id=$1 AND ($2='' OR namespace=$2) ORDER BY namespace, key`, missionID, ns)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.BoardEntry
	for rows.Next() {
		var e store.BoardEntry
		if err := rows.Scan(&e.MissionID, &e.Namespace, &e.Key, &e.Value, &e.Version, &e.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, &e)
	}
	return out, rows.Err()
}

// ---- Artifact（M2） ----

func (s *Store) PutArtifact(ctx context.Context, a *store.Artifact, now time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx,
		`INSERT INTO artifacts (id, sha256, uri, schema_ref, produced_by, mission_id, size, created_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		a.ID, a.SHA256, "blob://"+a.SHA256, a.SchemaRef, a.ProducedBy, a.MissionID, a.Size, now); err != nil {
		return store.ErrConflict
	}
	if err := putMeterTx(ctx, tx, &store.MeterRecord{ID: "meter:artifact:" + a.ID,
		MissionID: a.MissionID, SubtaskID: a.ProducedBy, Resource: "artifact.byte",
		Quantity: float64(a.Size), Trust: store.MeterAuthoritative}, now); err != nil {
		return err
	}
	ev := &store.Event{AggregateID: a.ProducedBy, MissionID: a.MissionID, Type: "artifact.produced",
		Payload: map[string]any{"artifact_id": a.ID, "sha256": a.SHA256, "size": a.Size},
		Actor:   store.Actor{Kind: "system", ID: "artifact-store"}, Ts: now}
	if err := appendEvent(ctx, tx, ev); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) GetArtifact(ctx context.Context, id string) (*store.Artifact, error) {
	var a store.Artifact
	var producedBy, schemaRef *string
	err := s.pool.QueryRow(ctx,
		`SELECT id, sha256, mission_id, produced_by, schema_ref, size, created_at
		 FROM artifacts WHERE id=$1`, id).
		Scan(&a.ID, &a.SHA256, &a.MissionID, &producedBy, &schemaRef, &a.Size, &a.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if producedBy != nil {
		a.ProducedBy = *producedBy
	}
	if schemaRef != nil {
		a.SchemaRef = *schemaRef
	}
	return &a, err
}

func (s *Store) RecordQuality(ctx context.Context, q *store.QualityRecord, signals []store.ReputationSignal,
	actor store.Actor, now time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	layers, _ := js(q.Layers)
	verifiedBy, _ := js(q.VerifiedBy)
	_, err = tx.Exec(ctx, `INSERT INTO quality_records
		(artifact_id, mission_id, subtask_id, producer_agent_id, producer_platform, attempt,
		 layers, score, confidence, verdict, failure_class, rubric, context_hash, verified_by, created_at)
		VALUES ($1,$2,NULLIF($3,''),NULLIF($4,''),$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
		q.ArtifactID, q.MissionID, q.SubtaskID, q.ProducerAgentID, q.ProducerPlatform, q.Attempt,
		layers, q.Score, q.Confidence, q.Verdict, q.FailureClass, q.Rubric, q.ContextHash, verifiedBy, now)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return store.ErrDuplicate
		}
		return err
	}
	for _, sig := range signals {
		if err := applyReputationSignalTx(ctx, tx, sig, now); err != nil && !errors.Is(err, store.ErrDuplicate) {
			return err
		}
	}
	if err := putMeterTx(ctx, tx, &store.MeterRecord{ID: "meter:verify:" + q.ArtifactID,
		MissionID: q.MissionID, SubtaskID: q.SubtaskID, Resource: "verify.call", Quantity: 1,
		Trust: store.MeterAuthoritative}, now); err != nil {
		return err
	}
	if err := appendEvent(ctx, tx, &store.Event{AggregateID: q.ArtifactID, MissionID: q.MissionID,
		Type: "artifact.verified", Payload: map[string]any{"artifact_id": q.ArtifactID,
			"score": q.Score, "confidence": q.Confidence, "verdict": q.Verdict,
			"failure_class": q.FailureClass}, Actor: actor, Ts: now}); err != nil {
		return err
	}
	q.CreatedAt = now
	return tx.Commit(ctx)
}

func (s *Store) GetQuality(ctx context.Context, artifactID string) (*store.QualityRecord, error) {
	var q store.QualityRecord
	var layers, verifiedBy []byte
	var subtaskID, producerID *string
	err := s.pool.QueryRow(ctx, `SELECT artifact_id, mission_id, subtask_id, producer_agent_id,
		producer_platform, attempt, layers, score, confidence, verdict, failure_class, rubric,
		context_hash, verified_by, created_at FROM quality_records WHERE artifact_id=$1`, artifactID).
		Scan(&q.ArtifactID, &q.MissionID, &subtaskID, &producerID, &q.ProducerPlatform,
			&q.Attempt, &layers, &q.Score, &q.Confidence, &q.Verdict, &q.FailureClass,
			&q.Rubric, &q.ContextHash, &verifiedBy, &q.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if subtaskID != nil {
		q.SubtaskID = *subtaskID
	}
	if producerID != nil {
		q.ProducerAgentID = *producerID
	}
	_ = json.Unmarshal(layers, &q.Layers)
	_ = json.Unmarshal(verifiedBy, &q.VerifiedBy)
	return &q, nil
}

func (s *Store) PutMeterRecord(ctx context.Context, m *store.MeterRecord, now time.Time) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	store.PriceMeter(m)
	m.RecordedAt = now
	metadata, _ := js(m.Metadata)
	tag, err := tx.Exec(ctx, `INSERT INTO meter_records
		(id, mission_id, subtask_id, agent_id, resource, quantity, unit, trust,
		 price_book, unit_price, credits, metadata, recorded_at)
		VALUES ($1,$2,NULLIF($3,''),NULLIF($4,''),$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (id) DO NOTHING`, m.ID, m.MissionID, m.SubtaskID, m.AgentID, m.Resource,
		m.Quantity, m.Unit, m.Trust, m.PriceBook, m.UnitPrice, m.Credits, metadata, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return store.ErrDuplicate
	}
	return tx.Commit(ctx)
}

func putMeterTx(ctx context.Context, tx pgx.Tx, m *store.MeterRecord, now time.Time) error {
	store.PriceMeter(m)
	m.RecordedAt = now
	metadata, _ := js(m.Metadata)
	_, err := tx.Exec(ctx, `INSERT INTO meter_records
		(id, mission_id, subtask_id, agent_id, resource, quantity, unit, trust,
		 price_book, unit_price, credits, metadata, recorded_at)
		VALUES ($1,$2,NULLIF($3,''),NULLIF($4,''),$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (id) DO NOTHING`, m.ID, m.MissionID, m.SubtaskID, m.AgentID, m.Resource,
		m.Quantity, m.Unit, m.Trust, m.PriceBook, m.UnitPrice, m.Credits, metadata, now)
	return err
}

func recordLeaseMeter(ctx context.Context, tx pgx.Tx, sub *mission.Subtask, agentID string, now time.Time) error {
	var createdAt time.Time
	if err := tx.QueryRow(ctx, `SELECT created_at FROM leases WHERE id=$1`, sub.LeaseID).Scan(&createdAt); err != nil {
		return err
	}
	quantity := float64(now.Sub(createdAt).Milliseconds())
	if quantity < 0 {
		quantity = 0
	}
	return putMeterTx(ctx, tx, &store.MeterRecord{ID: "meter:lease:" + sub.LeaseID,
		MissionID: sub.MissionID, SubtaskID: sub.ID, AgentID: agentID,
		Resource: "lease.wall_ms", Quantity: quantity, Trust: store.MeterAuthoritative}, now)
}

func (s *Store) ListMeterRecords(ctx context.Context, missionID string) ([]*store.MeterRecord, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM missions WHERE id=$1)`, missionID).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, store.ErrNotFound
	}
	rows, err := s.pool.Query(ctx, `SELECT id, mission_id, subtask_id, agent_id, resource,
		quantity, unit, trust, price_book, unit_price, credits, metadata, recorded_at
		FROM meter_records WHERE mission_id=$1 ORDER BY recorded_at, id`, missionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.MeterRecord
	for rows.Next() {
		var m store.MeterRecord
		var subtaskID, agentID *string
		var metadata []byte
		if err := rows.Scan(&m.ID, &m.MissionID, &subtaskID, &agentID, &m.Resource,
			&m.Quantity, &m.Unit, &m.Trust, &m.PriceBook, &m.UnitPrice, &m.Credits,
			&metadata, &m.RecordedAt); err != nil {
			return nil, err
		}
		if subtaskID != nil {
			m.SubtaskID = *subtaskID
		}
		if agentID != nil {
			m.AgentID = *agentID
		}
		_ = json.Unmarshal(metadata, &m.Metadata)
		out = append(out, &m)
	}
	return out, rows.Err()
}

// ---- 事件 ----

func (s *Store) ListMissionEvents(ctx context.Context, missionID string, afterSeq int64, limit int) ([]*store.Event, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT seq, aggregate_id, mission_id, type, payload, actor, ts FROM events
		 WHERE mission_id=$1 AND seq>$2 ORDER BY seq LIMIT $3`, missionID, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*store.Event
	for rows.Next() {
		var e store.Event
		var payload, actor []byte
		var mid *string
		if err := rows.Scan(&e.Seq, &e.AggregateID, &mid, &e.Type, &payload, &actor, &e.Ts); err != nil {
			return nil, err
		}
		if mid != nil {
			e.MissionID = *mid
		}
		_ = json.Unmarshal(payload, &e.Payload)
		_ = json.Unmarshal(actor, &e.Actor)
		out = append(out, &e)
	}
	return out, rows.Err()
}
