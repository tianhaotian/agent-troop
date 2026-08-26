-- M7C：Mission token 预算账户与 delegated subtask 原子 hold 生命周期（设计 §15.5）。

BEGIN;

CREATE TABLE IF NOT EXISTS mission_budgets (
    mission_id      TEXT PRIMARY KEY REFERENCES missions(id) ON DELETE CASCADE,
    total_tokens    BIGINT NOT NULL CHECK (total_tokens > 0),
    held_tokens     BIGINT NOT NULL DEFAULT 0 CHECK (held_tokens >= 0),
    spent_tokens    BIGINT NOT NULL DEFAULT 0 CHECK (spent_tokens >= 0),
    version         BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL,
    CHECK (held_tokens + spent_tokens <= total_tokens)
);

CREATE TABLE IF NOT EXISTS budget_holds (
    id              TEXT PRIMARY KEY,
    mission_id      TEXT NOT NULL REFERENCES missions(id) ON DELETE CASCADE,
    subtask_id      TEXT NOT NULL UNIQUE REFERENCES subtasks(id) ON DELETE CASCADE,
    attempt         INT NOT NULL DEFAULT 0,
    amount_tokens   BIGINT NOT NULL CHECK (amount_tokens > 0),
    actual_tokens   BIGINT NOT NULL DEFAULT 0 CHECK (actual_tokens >= 0),
    status          TEXT NOT NULL CHECK (status IN ('HELD','SETTLED','RELEASED')),
    created_at      TIMESTAMPTZ NOT NULL,
    settled_at      TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_budget_holds_mission
    ON budget_holds(mission_id, created_at, id);
CREATE INDEX IF NOT EXISTS idx_budget_holds_active
    ON budget_holds(mission_id) WHERE status = 'HELD';

COMMIT;
