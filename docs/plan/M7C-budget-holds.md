# M7C 实现计划：Mission 预算预占与原子结算

> 对应设计 §15.5、§21.3、§21.4。本切片为主子委托建立 token 预算硬顶：
> delegate 时原子 hold，成功按实际用量 settle，最终失败或取消 release。

## 1. 范围（In Scope）

| # | 切片 | 内容 | 验收标准 |
|---|------|------|---------|
| B1 | Mission 预算账户 | Mission 创建时可选 `budget_tokens`；独立账户记录 total/held/spent/version | 未配置预算的历史调用保持不限额；配置值必须大于 0 |
| B2 | delegate 原子 hold | budgeted Mission 的每个 delegate 必须声明正数 `budget_tokens`；子任务创建、预算校验、hold、幂等键和事件同事务提交 | 并发委托总 hold 不超过可用预算；失败不落子任务且不消耗幂等键 |
| B3 | 完成原子结算 | complete 可上报 `usage_tokens`；任务成功、hold 结算、余额更新、租约释放和事件同事务提交 | 少用退款；多用补扣；补扣导致超额时整个完成回滚；幂等重放不重复结算 |
| B4 | 失败/取消释放 | 委托任务最终失败或被取消时，在状态迁移事务内释放仍活跃的 hold | 可重试失败保留 hold；最终失败和取消只释放一次 |
| B5 | 可观测查询 | 提供 Mission 预算查询，返回账户余额和 hold 明细 | API 可区分 metered/unmetered，并展示 held/settled/released 生命周期 |

## 2. 关键语义

1. **兼容边界**：未配置 `budget_tokens` 的 Mission 没有预算账户，继续视为 unmetered；已有 API/测试无需强制迁移；
2. **预算归属固定**：递归委托始终从 child 所属 Mission 的同一账户预占，不能通过父子层级洗白预算；
3. **有预算必有切片**：metered Mission 的 delegate 缺少或传入非正数预算切片时拒绝，防止绕过硬顶；
4. **账户行串行化**：PostgreSQL 对 `mission_budgets` 行 `FOR UPDATE`，校验 `total - held - spent` 后更新，消除并发超支竞态；
5. **hold 一任务一条**：`budget_holds.subtask_id` 唯一；delegate 幂等键在预算变更前检查，重放不会二次 hold；
6. **完成多退少补**：settle 时先释放本 hold，再把 `usage_tokens` 计入 spent；若 `spent + 其他 hold + actual > total`，完成整体拒绝；
7. **重试不重复预占**：中间失败仍可能回到 READY，因此保留原 hold；只有达到重试上限的最终失败才 release；
8. **所有余额变化留痕**：`budget.held`、`budget.settled`、`budget.released` 与业务变更同事务写入事件流。

## 3. 存储与 API

- 新迁移 `0008_budget.sql`：
  - `mission_budgets(mission_id, total_tokens, held_tokens, spent_tokens, version, updated_at)`；
  - `budget_holds(id, mission_id, subtask_id, attempt, amount_tokens, actual_tokens, status, created_at, settled_at)`；
- Store 新增预算查询，并为 delegate/complete/fail/cancel 原子操作接入 hold 生命周期；
- API：
  - `POST /v1/missions` / `POST /v1/intents` 支持 `budget_tokens`；
  - delegate 的 `task.budget_tokens` 为预估切片；
  - `POST /v1/subtasks/{id}/complete` 支持 `usage_tokens`；
  - `GET /v1/missions/{id}/budget` 返回账户和 hold。

## 4. 明确不做

- 金额、多模型价格换算、租户账单、外部计费系统对账；本期只做 token 记账；
- hold TTL 自动释放：任务租约/最终状态仍是唯一释放依据，避免运行中任务被错误退款；
- 初始 DAG 根任务预占、上下文摄入计费、Verifier 预算比例；这些在预算底座稳定后逐步接入；
- 权限 grant 衰减与完整上下文包，留给后续 M7D。

## 5. DoD

- memory/core/API 测试覆盖预占、并发不超支、少用退款、多用补扣、幂等、重试、最终失败和取消；
- PG 集成测试覆盖迁移和原子生命周期；
- `go test ./...`、`go vet ./...`、`go test -race ./internal/...`、`go test -tags e2e ./e2e/`、`git diff --check` 全绿。
