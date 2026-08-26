-- 0003_m3.sql — M3：WAITING 挂起-唤醒（§7.3）与检查点续跑（§5.4）
-- checkpoint 为透明 JSON 载荷（progress/suspend 时落库，offer 下发供续跑）；
-- wake_* 为一次性唤醒注册（timer|manual + TTL，过期由 sweeper 置 FAILED）。

ALTER TABLE subtasks
	ADD COLUMN IF NOT EXISTS checkpoint     jsonb,
	ADD COLUMN IF NOT EXISTS wake_kind      text,
	ADD COLUMN IF NOT EXISTS wake_at        timestamptz,
	ADD COLUMN IF NOT EXISTS wake_deadline  timestamptz;

-- sweeper 扫描：timer 到期唤醒 / TTL 过期回收（部分索引，仅 WAITING 行）
CREATE INDEX IF NOT EXISTS idx_subtasks_waiting_due ON subtasks (wake_at)
    WHERE state = 'WAITING';
CREATE INDEX IF NOT EXISTS idx_subtasks_waiting_ttl ON subtasks (wake_deadline)
    WHERE state = 'WAITING';
