-- M2：决策工单增强 + 黑板（设计 §8.2 / §6.2）
BEGIN;

-- decisions 扩展为完整工单模型
ALTER TABLE decisions
    ADD COLUMN IF NOT EXISTS kind        TEXT NOT NULL DEFAULT 'decision',
    ADD COLUMN IF NOT EXISTS status      TEXT NOT NULL DEFAULT 'pending',
    ADD COLUMN IF NOT EXISTS deadline    TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS on_timeout  TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_decisions_pending ON decisions(status) WHERE status = 'pending';

-- 黑板：Mission 级 KV，CAS 版本防脏写
CREATE TABLE IF NOT EXISTS board_entries (
    mission_id  TEXT NOT NULL REFERENCES missions(id),
    namespace   TEXT NOT NULL,
    key         TEXT NOT NULL,
    value       JSONB NOT NULL,
    version     BIGINT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (mission_id, namespace, key)
);

-- artifacts 补尺寸列
ALTER TABLE artifacts ADD COLUMN IF NOT EXISTS size BIGINT NOT NULL DEFAULT 0;

COMMIT;
