-- M1 初始 schema（设计文档 §4.2 / §4.3）
-- 原则：状态迁移走条件更新（乐观锁 version）；事件只追加；租约带 fencing token。

BEGIN;

CREATE TABLE IF NOT EXISTS missions (
    id              TEXT PRIMARY KEY,            -- msn_...
    owner           TEXT NOT NULL,
    goal            TEXT NOT NULL,
    constraints     JSONB NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL,               -- ACTIVE | SUCCEEDED | FAILED | CANCELLED
    version         BIGINT NOT NULL DEFAULT 0,   -- 乐观锁
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS subtasks (
    id              TEXT PRIMARY KEY,            -- sub_...
    mission_id      TEXT NOT NULL REFERENCES missions(id),
    parent_id       TEXT REFERENCES subtasks(id),
    kind            TEXT NOT NULL,               -- agent | human_approval | human_decision | aggregation | condition
    spec            JSONB NOT NULL DEFAULT '{}', -- required_skills / input / output_schema / acceptance
    scheduling      JSONB NOT NULL DEFAULT '{}', -- priority / deadline / affinity / budget
    retry           JSONB NOT NULL DEFAULT '{}', -- max_attempts / backoff / on_failure
    state           TEXT NOT NULL,               -- 状态机见 internal/mission
    depends_on      TEXT[] NOT NULL DEFAULT '{}',-- DAG 边（上游 subtask id）
    assignee_agent_id TEXT,
    lease_id        TEXT,
    attempt         INT NOT NULL DEFAULT 0,
    result_ref      TEXT,
    version         BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_subtasks_mission ON subtasks(mission_id);
CREATE INDEX IF NOT EXISTS idx_subtasks_state   ON subtasks(state) WHERE state IN ('READY','WAITING','BLOCKED');

CREATE TABLE IF NOT EXISTS agents (
    id              TEXT PRIMARY KEY,            -- agt_...
    name            TEXT NOT NULL,
    platform        TEXT NOT NULL,               -- openclaw | hermes | custom | http-echo
    endpoint        JSONB NOT NULL DEFAULT '{}', -- type / uri / auth_ref
    capabilities    JSONB NOT NULL DEFAULT '[]', -- [{skill, level}]
    constraints     JSONB NOT NULL DEFAULT '{}', -- max_concurrency / data_boundary / cost
    health          JSONB NOT NULL DEFAULT '{}', -- status / last_heartbeat
    reputation      JSONB NOT NULL DEFAULT '{}', -- M2 后由信誉系统维护
    version         BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (
    id              TEXT PRIMARY KEY,            -- les_...
    subtask_id      TEXT NOT NULL REFERENCES subtasks(id),
    agent_id        TEXT NOT NULL REFERENCES agents(id),
    fencing_token   BIGINT NOT NULL,             -- 单调递增，防僵尸写入（§4.3）
    expires_at      TIMESTAMPTZ NOT NULL,
    state           TEXT NOT NULL,               -- ACTIVE | EXPIRED | FENCED | RELEASED
    created_at      TIMESTAMPTZ NOT NULL
);
-- 同一子任务同一时刻至多一个活跃租约
CREATE UNIQUE INDEX IF NOT EXISTS idx_leases_one_active
    ON leases(subtask_id) WHERE state = 'ACTIVE';

CREATE TABLE IF NOT EXISTS artifacts (
    id              TEXT PRIMARY KEY,            -- art_...
    sha256          TEXT NOT NULL,
    uri             TEXT NOT NULL,
    schema_ref      TEXT,
    produced_by     TEXT,                        -- subtask id
    mission_id      TEXT REFERENCES missions(id),
    meta            JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL
);

-- 事件日志：只追加（事件溯源，§4.3）。M1 落 PG 分区表起步（§22.4）。
CREATE TABLE IF NOT EXISTS events (
    seq             BIGSERIAL PRIMARY KEY,
    aggregate_id    TEXT NOT NULL,               -- mission / subtask / agent id
    mission_id      TEXT,                        -- 冗余便于 Mission 级 SSE 查询
    type            TEXT NOT NULL,               -- mission.created / subtask.leased / ...
    payload         JSONB NOT NULL DEFAULT '{}',
    actor           JSONB NOT NULL DEFAULT '{}', -- {kind, id}
    ts              TIMESTAMPTZ NOT NULL,
    trace_id        TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_aggregate ON events(aggregate_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_mission   ON events(mission_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_type      ON events(type);

-- fencing token 全局单调序列（§4.3）
CREATE SEQUENCE IF NOT EXISTS fencing_seq START 1;

-- 幂等键（Agent 结果上报去重，§4.3）
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key         TEXT PRIMARY KEY,
    result      TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS decisions (
    id              TEXT PRIMARY KEY,
    mission_id      TEXT NOT NULL REFERENCES missions(id),
    subtask_id      TEXT REFERENCES subtasks(id),
    decider_type    TEXT NOT NULL,               -- human | agent
	decider_id      TEXT,                        -- pending 工单尚无裁决者
    question        TEXT NOT NULL,
    options         JSONB NOT NULL DEFAULT '[]',
    choice          TEXT,
    rationale       TEXT,
    ts              TIMESTAMPTZ NOT NULL
);

COMMIT;
