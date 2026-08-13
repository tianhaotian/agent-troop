-- M5-H2：触发 scope 三级授权（§7.2① / §7.4）。
-- trigger_scopes 注册时显式声明，缺省 []（默认收紧：Agent 默认不能
-- 经 /v1/intents create_mission/wake）。
ALTER TABLE agents ADD COLUMN IF NOT EXISTS trigger_scopes JSONB NOT NULL DEFAULT '[]';
