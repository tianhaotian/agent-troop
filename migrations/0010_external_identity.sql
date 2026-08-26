-- M8A：外部身份 subject 与 Agent 注册表绑定。

BEGIN;

ALTER TABLE agents ADD COLUMN IF NOT EXISTS auth_subject TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_agents_auth_subject
    ON agents(auth_subject) WHERE auth_subject IS NOT NULL;

COMMIT;
