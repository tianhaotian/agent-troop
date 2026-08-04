-- 0004_m4.sql — M4：event/condition 唤醒注册载荷（§14.2/§14.3）
-- wake_spec 保存完整唤醒注册（event.types/where/after_seq、condition.board/op/value），
-- kind/at/deadline 仍用 0003 的顶层列做索引查询。

ALTER TABLE subtasks ADD COLUMN wake_spec jsonb;

-- event/condition 等待的 sweeper 扫描（部分索引，仅 WAITING 行）
CREATE INDEX idx_subtasks_waiting_kind ON subtasks (wake_kind)
    WHERE state = 'WAITING';
