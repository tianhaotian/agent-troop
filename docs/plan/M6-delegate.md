# M6 实现计划：主子委托协议核心（delegate + review/rework）

> 对应设计文档 §6.4.2（主子模式）、§15.1（正常委派时序）、§7.4(1)（trigger.spawn_subtask scope）。
> 流程约定同前：本计划先行；每个切片 `go test ./...` 全绿才标记完成。
> 前置：M4 已交付 event 唤醒（Lead 可 suspend 等子任务完成事件）、M5 已交付 scope
> 授权管道（`trigger.spawn_subtask` 常量预留）。
> 范围决策（2026-08-14 与用户确认）：M6 只做主子委托核心；A2A/MCP 适配、预算池、
> 上下文包、Lead 容错等移至 M7+。

## 1. 范围（In Scope）

| # | 切片 | 内容 | 设计章节 |
|---|------|------|---------|
| K1 | delegate 准入动作 | intents 管道增 `action=delegate`：Lead（RUNNING 中的 Agent）在同 Mission 内派生子女任务。强制 `trigger.spawn_subtask` scope + fencing 校验（父任务须 RUNNING 且由该 Lead 持约）+ 幂等键去重；`parent_id` 因果链；max_depth/max_fanout 校验；子任务即建即 READY 参与调度 | §15.1, §7.4(1) |
| K2 | review/rework 闭环 | Lead 验收子女任务结果：accept 无需动作（子任务已 SUCCEEDED）；rework = 带 `rework_of` + `feedback` 的再委托（链式重派，上限 max_rework）；`subtask.succeeded` 事件载荷补 `subtask_id`，Lead 以 event 唤醒 + where 谓词精确等待特定子女 | §15.1 |

## 2. 明确不做（M6 边界）

- **Lead 收件箱与显式 ingest**（§15.1 要点一）：MVP 结果经 `result_ref` + 黑板直读，收件箱闸门随上下文包（§16）一起做；
- **计划快照 / Lead 可替换**（§15.2）、**Sub 澄清路由**（§15.3）、**Lead 失联 takeover**（§15.4）：依赖心跳快照与收件箱体系，M7+；
- **预算池原子预扣**（§15.5）、**权限包络衰减**（§7.4(3)）：预算/密级数据模型未建，M7+；delegate 当前只做深度/扇出结构校验；
- **局部取消传播**（§15.6）：Mission 级联取消已有（M1），子树取消随主子容错一起做；
- **A2A/MCP 适配、托管 Adapter**：M7；
- delegate 的子女类型仅限 `agent` 子任务（human 节点委托待收件箱体系）。

## 3. 关键语义决策

1. **rework 以链式重派实现，不改状态机**：§15.1 的"sub_7 → READY (attempt+1)"要求终态可逆，与 M1 状态机迁移矩阵（终态不可逆，全库一致性的根基假设）冲突。M6 的 rework = 以 `rework_of` 引用原子任务派生新子任务，`feedback` 入子女 input——每次验收尝试独立成节点、天然可审计；`max_rework` 沿 rework_of 链计数，到达上限返回结构化错误由 Lead 换方案（升级人决策走已有 request_decision）；
2. **委托关系用 parent_id 而非 depends_on 表达**：子女不参与 DAG 依赖传播（依赖是"Lead 的委托决策"而非数据先后），即建即 READY；Mission 终态推导天然正确——子女非终态时 Mission 不会完结；
3. **fencing 即委托权**：delegate 必须持父任务的活跃租约（fencing token + version 校验，与 start/complete 同规格）——防止非执行中的 Agent 冒名派生，也天然保证"只有 RUNNING 中的 Lead 能 delegate"（§15.1 时序约束）；
4. **幂等键复用 intent- 命名空间**：delegate 必填 idempotency_key，result=子女 subtask id；重发返回原子女 + `deduplicated:true`（与 create_mission 语义一致，§15.1"所有消息携带幂等键"）；
5. **fencing + 幂等 + 落库同一原子操作**（沿用 CompleteSubtask 模式）：`SpawnSubtask` 在 store 层一个事务内完成幂等键检查（撞键返回 ErrDuplicate + 原子女 ID）→ 父任务 fencing/RUNNING 校验 → 子女插入 → 事件追加——无占位窗口，重试/并发双发恰好一次。结构校验（depth/fanout/max_rework）在 core 层先于 store 调用，失败不消耗键（同 M4 早失败原则）；管道顺序：鉴权（M5）→ core 校验 → store 原子落库；
6. **depth/fanout 配置化**：`Config.MaxDelegateDepth`（默认 4）、`MaxDelegateFanout`（默认 8）、`MaxRework`（默认 3）；depth 沿 parent_id 链上溯计算，fanout 按同父子任务计数；
7. **事件留痕（§7.4(6) 因果审计）**：子女创建事件 payload 带 `parent_subtask_id` / `rework_of` / actor=Lead agent——从任一子任务可沿事件反查完整触发链；`subtask.succeeded` 载荷补 `subtask_id`（双 store），Lead `wake_on.event.where={"subtask_id":"sub_7"}` 精确等待。

## 4. 模型与存储变更

- `mission.Subtask` 增 `Input map[string]any`（delegate 子女的任务载荷；pg 落 spec jsonb，**无需改表**）与 `ReworkOf string`（同）；ParentID 列已存在（0001）；
- Store 接口新增：
  - `SpawnSubtask(ctx, idemKey, parentID string, fencingToken, parentVersion int64, child *mission.Subtask, actor Actor, now time.Time) (existingID string, err error)`——原子完成（CompleteSubtask 同构）：幂等键撞键返回 ErrDuplicate + 原子女 ID；父任务 fencing + RUNNING 校验 → 子女插入（PENDING）→ 事件追加；
  - `CountChildren(ctx, parentID string) (int, error)`（fanout 校验）；
  - `subtask.succeeded` 载荷增 `subtask_id`（pg + memory 双实现）；
- core：`Service.Delegate(ctx, in Intent)` 编排水线 + depth 计算（沿 ParentID 上溯，上限 MaxDelegateDepth 截断防环）；
- API：Intent 增 `parent_subtask_id` / `fencing_token` / `parent_version` / `task`（TaskSpec + rework_of + feedback）字段；action=delegate 接入 M5 鉴权管道（scope=trigger.spawn_subtask）。

## 5. 测试方案

- core：delegate 全链路（scope 403 / 未注册拒 / fencing 错拒 / 父任务非 RUNNING 拒 / Mission 终态拒）；子女即 READY 可被调度；幂等重发返回同子女；depth 超限拒（构造 parent 链）；fanout 超限拒；rework 链（rework_of + feedback 入 input，第三次后 max_rework 拒）；delegate 后 Lead suspend 等 `subtask.succeeded` where subtask_id 精确唤醒；
- store（memory/pg）：SpawnSubtask fencing/CAS/原子性；CountChildren；succeeded 事件载荷含 subtask_id；
- e2e（`-tags e2e`）：主子循环全程——Lead 领任务 → delegate 派生 → Worker 执行子女 → Lead event 唤醒 → 查看结果 → rework 一次（带 feedback）→ 新子女完成 → accept，Mission SUCCEEDED。

**DoD**：`go test ./...` + `go test -tags e2e ./e2e/` 全绿；README 补 delegate/rework 示例；无需新迁移（spec jsonb 内演进）。
