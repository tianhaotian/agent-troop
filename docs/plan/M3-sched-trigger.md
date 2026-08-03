# M3 实现计划：策略插件化 + 挂起-唤醒（Continuation）+ 检查点续跑

> **状态：✅ 已交付（2026-08-04）** — `go test ./...` 与 `go test -tags e2e ./e2e/` 全绿。
> 对应设计文档 §12.2 M3、§7.3/§14.4（挂起-唤醒语义）、§5.2（调度策略）、§5.4（检查点）。
> 流程约定同 M1/M2：本计划先行；每个切片 `go test ./...` 全绿才标记完成。

## 1. 范围（In Scope）

| # | 切片 | 内容 | 设计章节 |
|---|------|------|---------|
| T1 | 放置策略插件化 | `PlacementStrategy` 接口（Filter/Score 两段）；内置 `capability-first`（现状抽出）与 `round-robin`；`TROOP_SCHEDULER` 环境变量选择 | §5.2 |
| T2 | Deadline/Priority 打分 | 打分公式加入优先级权重与 deadline 紧迫度（越紧迫加分越高）；就绪队列排序已具备（priority desc / deadline asc），本切片只动 score | §5.2 |
| T3 | 检查点续跑 | `POST /v1/subtasks/{id}/progress` 扩展携带 `checkpoint`（JSON）落库；Subtask 新增 `checkpoint` 字段并随 offer 下发（`resume_from` 语义）；suspend 亦可带检查点 | §5.4, §7.3 |
| T4 | WAITING 挂起-唤醒 | `POST /v1/subtasks/{id}/suspend`（fencing 校验，RUNNING→WAITING，**释放租约**，落 `wake_on`）；`POST /v1/subtasks/{id}/wake`（人工唤醒）；SweepOnce 处理 timer 到期唤醒（CAS WAITING→READY，可换 Agent 续跑）与 wake TTL 过期（FAILED reason=wake_timeout + 级联取消下游） | §7.3, §14.4 |

## 2. 明确不做（M3 边界）

- `wake_on` 的 event/condition 两类条件与 CEL 求值器（§14.3）——M3 仅支持 `timer`（at）与 `manual`；
- 触发准入管道（TaskIntent 归一化、scope 分级授权 §7.2）与事件模式匹配倒排索引（§14.2）——M4；
- 熔断、Ensemble 会诊、Cost 计费调度、指数退避 jitter；
- 唤醒鉴权：M3 `wake` 仍无认证（同 M2 决策收件箱定位）。

## 3. 关键语义决策

1. **挂起释放租约**：与 BLOCKED（人决策，保留租约等原 Agent 续跑）不同，WAITING 释放租约与并发额度——唤醒后重新走调度，**允许换 Agent 凭 checkpoint 续跑**（§14.4）；
2. **wake_on 必带 TTL**：`suspend` 必须给出 `wake_deadline`（平台不上限但必填），到期未唤醒 → FAILED(reason=wake_timeout)，走既有级联取消 + Mission 终态推导（防永久悬挂，§14.4）；
3. **恰好一次唤醒**：timer 唤醒用 CAS 状态迁移（WAITING→READY 校验 expected version）保证多副本 sweeper 竞争只醒一次；唤醒后 wake 字段清空，再次等待须重新 suspend（一次性注册语义）；
4. **检查点是透明载荷**：平台只存储/透传（`json.RawMessage`，≤64KB），不解释内容；续跑 Agent 自行从 offer 的 `subtask.checkpoint` 恢复；
5. **策略接口最小化**：`Score(sub, agent) (float64, bool)` 返回 (分数, 是否合格)，Filter 并入 Score（不合格即 false）；调度循环不变，便于后续 A/B；
6. **心跳即存活证明（实现时补充）**：Agent 被 sweeper 标记 suspect 后，再次心跳自动恢复 healthy（`down` 为人工/熔断标记，不自动恢复）——否则短暂失联的 Agent 永远无法回归调度池。

## 4. 模型与存储变更

- `Subtask` 新增：`Checkpoint json.RawMessage`、`WakeKind string`（timer|manual）、`WakeAt *time.Time`、`WakeDeadline *time.Time`；
- 迁移 `0003_m3.sql`：subtasks 增列 `checkpoint jsonb`、`wake_kind text`、`wake_at timestamptz`、`wake_deadline timestamptz`；
- Store 接口新增：
  - `SuspendSubtask`（fencing + RUNNING→WAITING + 释放租约 + 落 wake 字段/检查点，原子）；
  - `WakeSubtask`（CAS WAITING→READY + 清 wake 字段）；
  - `ListWaitingDue(now)`（timer 到期扫描）；
  - `ExpireWakes(now)`（TTL 过期 → FAILED，返回受影响子任务，由 core 级联取消 + 推导终态）；
  - `SaveCheckpoint`（progress 携带，fencing 校验，不迁移状态）。

## 5. 测试方案

- core：策略选择（round-robin 轮转分布）、deadline 紧迫度影响放置顺序、suspend→timer 唤醒→换 Agent 续跑（检查点可读回）、manual wake、wake TTL 过期→FAILED+级联、progress 检查点落库与 fencing 拒绝；
- store（memory）：SuspendSubtask 原子性（租约释放 + 事件）、WakeSubtask CAS 竞争、ListWaitingDue/ExpireWakes 边界；
- e2e（`-tags e2e`）：Agent 执行中 suspend(timer) → sweeper 唤醒 → 另一 Agent 接受 offer 并凭 checkpoint 续跑完成；TTL 过期路径 Mission FAILED。

**DoD**：`go test ./...` + `go test -tags e2e ./e2e/` 全绿；README 补充 suspend/wake/progress 新 API 示例与 `TROOP_SCHEDULER` 说明。
