# M7D 实现计划：权限包络衰减与可审计上下文包

> 对应设计 §7.4(3)、§15.1 与 §16。本切片完成 M7：把 delegate 的权限边界固化到
> Subtask 规格，并在每次 lease offer 时物化不可变、带 SHA-256 指纹的上下文包。

## 1. 范围与验收

| # | 切片 | 内容 | 验收标准 |
|---|------|------|---------|
| C1 | 权限包络 | Subtask grants 包含密级上限、tool scopes、artifact refs 与 board views | Mission 根任务可声明包络；格式非法在落库前返回 400 |
| C2 | 委托衰减 | delegated child 的 grants 必须是 parent grants 的子集 | 不能提升密级、增加 tool scope/artifact、扩大 board key 或把 ro 提升为 rw；拒绝不消耗幂等键/预算 |
| C3 | 上下文物化 | OfferLease 同事务生成 task/input、显式 artifact、授权 board 切片、相关决策摘要、预算与 wake 信息 | Sub 看不到兄弟任务或未 grant 数据；跨 Mission artifact 被过滤 |
| C4 | 快照审计 | 上下文包以 lease 为版本边界持久化，计算稳定 SHA-256，并追加 `context.materialized` 事件 | 相同内容指纹稳定；takeover/new lease 生成新包；包不可覆盖 |
| C5 | Adapter/API | offer 响应携带 context package；另提供按 lease 查询端点 | Agent 不需要遍历 Mission 黑板即可开始任务；未知 lease 返回 404 |

## 2. 权限语义

1. 密级顺序为 `public < internal < confidential < restricted`；child 的上限只能相等或降低；
2. 空 grants 是最小权限（`public`、无工具、无 artifact、无 board）；兼容旧任务但不会隐式暴露 Mission 数据；
3. tool scopes 与 artifact refs 使用集合子集判断；
4. board grant 为 `{namespace, keys, mode}`：空 keys 表示整个 namespace；child 只能取父级允许的 key，`rw` 需要父级也是 `rw`；
5. grant 校验发生在 `SpawnSubtask` 前，因此权限拒绝不会创建 child、hold 预算或占用幂等键；
6. 上下文物化再次按 `mission_id` 过滤 artifact，形成纵深防御，禁止跨 Mission 隐式共享；
7. 决策摘要只包含与当前 subtask 直接关联的状态/选择，不携带 rationale、decider 等 human-only 信息。

## 3. 存储与 API

- Subtask `spec` JSONB 增加 `grants`，无需改 subtasks 表；
- 新迁移 `0009_context_packages.sql`：按 lease 唯一保存 payload、snapshot_hash 与创建时间；
- Store 在 `OfferLease` 事务内写 context package 与事件，并新增 `GetContextPackage`；
- API：
  - `GET /v1/agents/{id}/offers` 的每个 offer 增加 `context_package`；
  - `GET /v1/leases/{id}/context` 查询不可变快照。

## 4. M7 完成边界

- M7A：生产可靠性基线；M7B：Lead 恢复；M7C：预算原子闭环；M7D：权限与上下文；
- A2A/MCP 协议适配、外部身份认证、签名 URL、密钥管理和托管 Adapter 属于 M8 集成阶段；
- 上下文摘要/检索系统任务与上下文 token 自动计费在后续质量与计量阶段接入。

## 5. DoD

- core/memory/PG/API/e2e 覆盖权限拒绝、最小知情、跨 Mission 隔离、hash 审计和新 lease 重物化；
- `go test ./...`、`go vet ./...`、`go test -race ./internal/...`、`go test -tags e2e ./e2e/`、`git diff --check` 全绿；
- 更新路线图为 M7 完成，提交并推送当前分支。
