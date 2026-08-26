# M7B 实现计划：Lead 收件箱、计划快照与失联接管

> 对应设计 §15.1、§15.2、§15.4 与 §16.3。M7A 已补齐 readiness、租约身份绑定和
> PostgreSQL CI；本切片实现主子模式最小可恢复闭环，不扩展预算池与完整上下文包。

## 1. 范围（In Scope）

| # | 切片 | 内容 | 验收标准 |
|---|------|------|---------|
| L1 | Lead inbox | delegated child 成功时，在同一事务内写入父 Lead 收件箱；Lead 可列出 pending/all 项，并在活跃租约保护下显式 `ingest(summary/full)` | 完成与入箱原子；幂等重放不重复入箱；非租约持有人不能 ingest |
| L2 | 心跳与计划快照 | `POST /v1/subtasks/{id}/lead/heartbeat` 原子完成 fencing/owner 校验、租约续期和 `lead-plan/{subtask_id}` 版本化快照写入 | 首次创建与 CAS 更新可验证；冲突不续租；快照大小受限且必须是 JSON |
| L3 | 确定性 takeover | sweeper 发现已过期的 RUNNING Lead 租约（存在计划快照或直接子女）后，原子 fence 旧租约、回收 Agent 并发计数、标记旧 Agent suspect、将 Lead 任务送回 READY | 旧 token 后续写入被拒；in-flight child 不受影响；Scheduler 可把 Lead 分配给健康继任者 |
| L4 | 恢复上下文 | 提供 Lead recovery context 查询，返回最近计划快照与完整收件箱（含 pending/ingested 状态），供继任者重建上下文 | takeover 前后内容一致，读取不改变 inbox 状态 |

## 2. 关键语义

1. **结果入箱与完成同事务**：child 的 `SUCCEEDED`、result_ref、租约释放、成功事件和 inbox enqueue 必须一起提交；
2. **显式 ingest 是上下文闸门**：列表查询不等于消费，只有 ingest 才把条目标为已摄入，并记录模式、Agent 和时间；
3. **快照 CAS 与续租同成败**：错误 snapshot version 不得延长 Lead 租约，防止僵尸 Lead 靠冲突心跳维持存活；
4. **takeover 使用新 fencing token**：旧租约置 `FENCED`，Lead 子任务 `RUNNING→READY` 并清空 assignee/lease；新 Scheduler offer 生成全局递增 token；
5. **Lead 识别不引入新任务类型**：当前兼容 M6——存在 `lead-plan` 快照或直接子女的 RUNNING Agent 任务视为 Lead；显式 `kind=coordinator`/`can_delegate` 留待协议模型升级；
6. **故障隔离**：takeover 不取消、不 fence 正在执行的 child；其结果继续进入同一父 Lead inbox。

## 3. 存储与 API

- 新迁移 `0007_lead_recovery.sql`：`lead_inbox` 表，`source_subtask_id` 唯一保证恰好一次入箱；
- Store 新增 inbox list/ingest、原子 snapshot heartbeat、stale Lead takeover；
- API：
  - `GET /v1/subtasks/{id}/lead/inbox?status=pending|all`
  - `POST /v1/subtasks/{id}/lead/inbox/{item}/ingest`
  - `POST /v1/subtasks/{id}/lead/heartbeat`
  - `GET /v1/subtasks/{id}/lead/context`

## 4. 明确不做

- Sub clarification/guidance 双向消息、预算 hold、权限 grant、完整上下文包与摘要生成；
- 自动 replan、局部取消或“无继任者”人工决策工单；无候选时 Lead 保持 READY 等待运维/后续调度；
- OIDC/签名身份认证；继续沿用 M7A 的租约归属校验边界。

## 5. DoD

- memory/core/API 测试覆盖 L1–L4 和故障路径；PG 集成测试覆盖迁移、入箱、快照 CAS、fence/takeover；
- `go test ./...`、`go vet ./...`、`go test -race ./internal/...`、`go test -tags e2e ./e2e/`、`git diff --check` 全绿；
- PostgreSQL 16 的真实执行由 M7A CI 强制门禁。
