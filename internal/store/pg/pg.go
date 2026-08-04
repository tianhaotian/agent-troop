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

// ---- helpers ----

func js(v any) ([]byte, error) { return json.Marshal(v) }

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
		RequiredSkills []string `json:"required_skills"`
		Question       string   `json:"question"`
		Options        []string `json:"options"`
		OnTimeout      string   `json:"on_timeout"`
	}
	_ = json.Unmarshal(spec, &specObj)
	sub.RequiredSkills = specObj.RequiredSkills
	sub.Question = specObj.Question
	sub.Options = specObj.Options
	sub.OnTimeout = specObj.OnTimeout
	_ = json.Unmarshal(scheduling, &sub.Scheduling)
	_ = json.Unmarshal(retry, &sub.Retry)
	return &sub, nil
}

// marshalSpec 序列化 subtask spec JSONB（required_skills + human 节点字段）。
func marshalSpec(sub *mission.Subtask) []byte {
	spec, _ := json.Marshal(map[string]any{
		"required_skills": sub.RequiredSkills,
		"question":        sub.Question,
		"options":         sub.Options,
		"on_timeout":      sub.OnTimeout,
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
	ev := &store.Event{AggregateID: m.ID, MissionID: m.ID, Type: "mission.created",
		Payload: map[string]any{"goal": m.Goal, "owner": m.Owner}, Actor: actor, Ts: now}
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
			string(sub.State), sub.DependsOn, sub.Attempt, now); err != nil {
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
		`SELECT id, owner, goal, status, version FROM missions WHERE id=$1`, id).
		Scan(&m.ID, &m.Owner, &m.Goal, &status, &m.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	m.Status = mission.MissionStatus(status)
	return &m, nil
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
	_, err := s.pool.Exec(ctx,
		`INSERT INTO agents (id, name, platform, endpoint, capabilities, constraints, health, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8)
		 ON CONFLICT (id) DO UPDATE SET name=$2, platform=$3, endpoint=$4, capabilities=$5,
		 constraints=$6, health=$7, updated_at=$8, version=agents.version+1`,
		a.ID, a.Name, a.Platform, endpoint, caps,
		fmt.Sprintf(`{"max_concurrency":%d}`, a.MaxConcurrency),
		fmt.Sprintf(`{"status":%q,"last_heartbeat":%q}`, health, now.Format(time.RFC3339Nano)), now)
	return err
}

func scanAgent(row pgx.Row) (*store.Agent, error) {
	var a store.Agent
	var caps, endpoint, constraints, health []byte
	err := row.Scan(&a.ID, &a.Name, &a.Platform, &endpoint, &caps, &constraints, &health, &a.Running)
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal(caps, &a.Capabilities)
	_ = json.Unmarshal(endpoint, &a.Endpoint)
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
		   jsonb_set(health,'{last_heartbeat}',to_jsonb($2::text)),
		   '{status}', to_jsonb(CASE WHEN health->>'status'='suspect' THEN 'healthy'
		                             ELSE health->>'status' END)),
		 updated_at=$2 WHERE id=$1`, id, now.Format(time.RFC3339Nano))
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
		 VALUES ('les_' || $1, $2, $3, $4, $5, 'ACTIVE', $5) RETURNING id`,
		fmt.Sprintf("%012d", fence), subtaskID, agentID, fence, now.Add(ttl)).Scan(&lease.ID)
	if err != nil {
		return nil, store.ErrConflict // 唯一部分索引：已有活跃租约
	}
	lease.SubtaskID, lease.AgentID, lease.FencingToken = subtaskID, agentID, fence
	lease.ExpiresAt, lease.State = now.Add(ttl), store.LeaseActive

	sub.State = mission.StateOffered
	sub.Assignee = agentID
	sub.LeaseID = lease.ID
	if err := updateSubtask(ctx, tx, sub, expectedVersion, now); err != nil {
		return nil, err
	}
	ev := &store.Event{AggregateID: subtaskID, MissionID: sub.MissionID, Type: string(mission.EvLeaseOffered),
		Payload: map[string]any{"state": string(mission.StateOffered), "agent_id": agentID,
			"lease_id": lease.ID, "fencing_token": fence}, Actor: actor, Ts: now}
	if err := appendEvent(ctx, tx, ev); err != nil {
		return nil, err
	}
	return &lease, tx.Commit(ctx)
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
		`SELECT id, subtask_id, agent_id, fencing_token, expires_at, state FROM leases WHERE id=$1`, id).
		Scan(&l.ID, &l.SubtaskID, &l.AgentID, &l.FencingToken, &l.ExpiresAt, &l.State)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, store.ErrNotFound
	}
	return &l, err
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
		`SELECT id, subtask_id FROM leases WHERE state='ACTIVE' AND expires_at<=$1 FOR UPDATE`, now)
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
		// 仅回收尚未开始执行的（OFFERED/LEASED）；RUNNING 的活性由心跳/熔断负责（M1 边界）
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
	if err := releaseLease(ctx, tx, sub.LeaseID); err != nil {
		return nil, err
	}
	ev := &store.Event{AggregateID: sub.ID, MissionID: sub.MissionID, Type: string(mission.EvCompleted),
		Payload: map[string]any{"state": string(mission.StateSucceeded), "result_ref": resultRef},
		Actor:   actor, Ts: now}
	if err := appendEvent(ctx, tx, ev); err != nil {
		return nil, err
	}
	sub.Version++
	return sub, tx.Commit(ctx)
}

func (s *Store) FailSubtask(ctx context.Context, id string, fencingToken int64, reason string,
	expectedVersion int64, actor store.Actor, now time.Time) (*mission.Subtask, error) {
	sub, err := s.fencedTransition(ctx, id, fencingToken, expectedVersion, mission.EvFailed,
		map[string]any{"reason": reason}, actor, now, nil)
	if err != nil {
		return nil, err
	}
	if _, err := s.pool.Exec(ctx, `UPDATE leases SET state='RELEASED' WHERE id=$1`, sub.LeaseID); err != nil {
		return nil, err
	}
	return sub, nil
}

func (s *Store) BlockSubtask(ctx context.Context, id string, fencingToken int64, expectedVersion int64,
	actor store.Actor, now time.Time) (*mission.Subtask, error) {
	return s.fencedTransition(ctx, id, fencingToken, expectedVersion, mission.EvBlocked,
		map[string]any{}, actor, now, nil)
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
		Type: string(mission.EvSuspended),
		Payload: map[string]any{"state": string(mission.StateWaiting), "wake_kind": wake.Kind},
		Actor: actor, Ts: now}); err != nil {
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
	if _, err := mission.Apply(sub.State, mission.EvWoken); err != nil {
		return nil, store.ErrConflict
	}
	sub.State = mission.StateReady
	sub.WakeKind, sub.WakeAt, sub.WakeDeadline, sub.WakeSpec = "", nil, nil, nil // 一次性注册，清空
	if err := updateSubtask(ctx, tx, sub, expectedVersion, now); err != nil {
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
			Type: string(mission.EvFailed),
			Payload: map[string]any{"state": string(mission.StateFailed), "reason": "wake_timeout"},
			Actor: store.Actor{Kind: "system", ID: "sweeper"}, Ts: now}); err != nil {
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

func (s *Store) getSubtask(ctx context.Context, id string) (*mission.Subtask, error) {
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
			if expectedVersion == -1 {
				// 约定 -1 表示"应不存在"，此处不会到达（>=0 分支）
			}
			// 可能是新建（expectedVersion 0 且不存在）：尝试插入
			if _, ierr := s.pool.Exec(ctx,
				`INSERT INTO board_entries (mission_id, namespace, key, value, version, updated_at)
				 VALUES ($1,$2,$3,$4,0,$5) ON CONFLICT DO NOTHING`,
				e.MissionID, e.Namespace, e.Key, val, now); ierr != nil {
				return nil, ierr
			}
			cur, gerr := s.BoardGet(ctx, e.MissionID, e.Namespace, e.Key)
			if gerr != nil {
				return nil, gerr
			}
			if cur.Version != expectedVersion && !(expectedVersion == 0 && cur.Version == 0) {
				return nil, store.ErrConflict
			}
			return cur, nil
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
		 WHERE mission_id=$1 AND namespace=$2 ORDER BY key`, missionID, ns)
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
