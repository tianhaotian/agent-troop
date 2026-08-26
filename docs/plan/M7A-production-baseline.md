# M7A 实现计划：生产可靠性基线

> M1–M6 的功能闭环已经形成；进入 Lead 收件箱与 takeover 前，先把部署探针、
> Agent 租约身份边界和 PostgreSQL CI 门禁补齐，避免在更复杂的协作协议上放大基础设施风险。

## 1. 范围（In Scope）

| # | 切片 | 内容 | 验收标准 |
|---|------|------|---------|
| R1 | Store readiness | Store 增加真实连通性探针；`GET /readyz` 对 memory 返回 ready，对 PostgreSQL 执行 `Ping`，失败返回 503；`/healthz` 仍只表示进程存活 | 单测覆盖 200/503；PG 实现可编译 |
| R2 | Agent 租约身份 | accept/start/progress/complete/fail/suspend/request_decision/delegate 在进入状态迁移前校验 `agent_id` 是目标租约持有人，且 token/目标子任务匹配；完成幂等重放允许已释放的原租约 | 冒用 Agent 返回 `ErrForbidden`/HTTP 403；旧 token 返回 `ErrFenced`；完成重放保持可恢复 |
| R3 | PostgreSQL CI 门禁 | GitHub Actions 启动 PostgreSQL 16，强制执行真实 PG Store 测试；迁移在同一数据库重复执行，验证 fresh/reapply；同时执行 unit、vet、race、e2e | CI 任一检查失败即失败，不允许因缺少 `TROOP_TEST_PG` 跳过 PG 测试 |

## 2. 明确不做

- 本切片不引入 OIDC、Human RBAC 或密钥签发；这里只封闭现有协议内可验证的租约身份关系；
- 不在本切片引入独立 Outbox 投递表、指标后端或告警平台；当前事件与状态的事务一致性保持不变；
- Lead inbox、计划快照、心跳接管与 takeover 属于下一切片 M7B。

## 3. 关键语义

1. **liveness 与 readiness 分离**：`/healthz` 不访问依赖，供进程存活检查；`/readyz` 探测 Store，依赖不可用时摘除流量但不触发无意义的进程重启；
2. **租约身份不是客户端声明**：`agent_id` 只用于指出调用者，最终以持久化 Lease 的 `AgentID` 为准；调用者、subtask、lease、fencing token 必须形成同一绑定；
3. **幂等优先语义不回退**：complete 成功后租约会 RELEASED。原 Agent 用同一幂等键重放时仍应抵达 Store 的幂等分支，因此服务层只校验绑定与 token，不强制该路径的租约仍 ACTIVE；
4. **迁移可重复执行**：所有迁移文件必须能在已升级数据库再次执行。CI 显式连续应用两遍，不依赖测试顺序偶然覆盖。

## 4. 测试与 DoD

- `go test ./...`
- `go vet ./...`
- `go test -race ./internal/...`
- `go test -tags e2e ./e2e/`
- 在 PostgreSQL 16 上设置 `TROOP_TEST_PG` 后执行 `go test -count=1 ./internal/store/pg`
- `git diff --check`

完成 R1–R3 后，下一阶段进入 M7B：Lead inbox（显式 ingest）、计划快照、Lead 心跳与确定性 takeover。
