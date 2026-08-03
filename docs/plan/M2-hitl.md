# M2 实现计划：人在回路 + 黑板 + Artifact Store

> **状态：✅ 已交付（2026-08-03）** — `go test ./...` 与 `go test -tags e2e ./e2e/` 全绿。
> 对应设计文档 §12.2 M2、§8（人在回路）、§6.2（黑板）、§4.1（Artifact Store）。
> 流程约定同 M1：本计划先行；每个切片 `go test ./...` 全绿才标记完成。

## 1. 范围（In Scope）

| # | 切片 | 内容 | 设计章节 |
|---|------|------|---------|
| H1 | human 节点编排 | `human_approval` / `human_decision` 子任务：READY 后不派 Agent，自动生成决策工单并置 BLOCKED；状态机补 `READY→BLOCKED` 边 | §3.2, §8 |
| H2 | 决策收件箱与裁决 | `GET /v1/decisions?status=pending`、`POST /v1/decisions/{id}/resolve`；裁决后流转：approve →（经 RUNNING）→ SUCCEEDED，reject → FAILED；决策落 `decisions` 表（审计） | §8.2 |
| H3 | Agent 主动决策请求 | `POST /v1/subtasks/{id}/request_decision`（fencing 校验，RUNNING→BLOCKED）；裁决 approve → 回 RUNNING 续跑 | §7.1 T6, §8 |
| H4 | 黑板 | Mission 级 KV：`PUT/GET /v1/missions/{id}/board/{ns}/{key}`，CAS 版本、命名空间隔离 | §6.2, §16.1 |
| H5 | Artifact Store | 注册表 + 本地 blob（sha256 内容寻址）：`POST /v1/artifacts`（上传）、`GET /v1/artifacts/{id}`（元数据）、`GET /v1/artifacts/{id}/content`（下载） | §4.1 |
| H6 | 决策超时 | 工单带 deadline + on_timeout（auto_approve / auto_reject / none）；SweepOnce 处理过期 | §8.2 |

## 2. 明确不做（M2 边界）

- 决策路由（按角色/值班表分配 approver）——M2 工单为全局收件箱，谁都可以裁决；
- 鉴权与脱敏（§8.3）：M2 仍无认证，本地开发定位；
- MinIO/S3 后端：blob 先落本地目录（`TROOP_BLOB_DIR`，默认 `./data/artifacts`），接口预留替换；
- Watcher 增强（SSE 已在 M1 交付）；React Console。

## 3. 关键语义决策

1. **审批节点的"执行"就是人的裁决本身**：裁决通过后状态机走 `BLOCKED →(decision_approved)→ RUNNING →(completed)→ SUCCEEDED` 两步落两个事件，保证审计链完整且无需改状态机终态语义；
2. **Agent 决策请求的续跑语义**：approve → `BLOCKED→RUNNING` 原 Agent 续跑（租约仍持有，BLOCKED 不释放租约）；reject → FAILED 走既有重试/终态逻辑；
3. **choice 约定**：选项中 `reject` 为唯一否决值，其余任何 choice 均视为批准并作为决策内容下发（Agent 可读回）；
4. **黑板写入 CAS**：`expected_version` 可空（空=盲写覆盖），非空不匹配返回 409（§16.3 脏写防护的最小版）；
5. **否决即终态（e2e 验收时补充）**：reject / 重试耗尽的永久失败会级联取消所有传递依赖它的 PENDING 下游（它们永不可就绪），再由 `MissionStatusOf` 推导 Mission FAILED——否则下游吊在 PENDING，Mission 永远 ACTIVE。级联实现见 `core.cancelUnreachable`。

## 4. 测试方案

- 状态机：`READY→BLOCKED` 新边 + 回归全矩阵；
- core：审批节点全生命周期（READY→工单→approve→SUCCEEDED→下游传播）；reject→FAILED→Mission FAILED；Agent request_decision→approve→续跑；超时 auto_reject；
- store（memory）：decisions CRUD + 裁决 CAS（重复裁决 409）、黑板 CAS、artifact 注册；
- e2e（`-tags e2e`）：三节点流水线 `生成(agent) → 审批(human) → 发布(agent)`，含一次 reject 重提交流程；黑板读写；artifact 上传下载校验 sha256。

**DoD**：`go test ./...` + `go test -tags e2e ./e2e/` 全绿；README 补充新 API 示例。
