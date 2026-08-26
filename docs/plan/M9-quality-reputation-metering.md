# M9：质量验证、信誉闭环、权威计量与可观测性

> 对齐设计文档 §11、§18、§19、§21。本里程碑以可持久化、可审计、可回放为底线，
> 不把只存在于进程内的统计量当作事实来源。

## 1. 范围与验收标准

| 工作包 | 范围 | 验收标准 |
|---|---|---|
| M9A Verifier | Artifact L0–L3 分层验收、结构化 verdict/failure class、证据和 rubric 版本、生产者/裁判隔离 | 同一 Artifact 验收幂等；非法分数、失败分类和自产自验被拒绝；写入 `artifact.verified` 事件 |
| M9B Reputation | `(agent_id, skill)` Beta 成功率、质量 EWMA、可靠性 Beta、时延/成本效率 EWMA | 验收与任务结果以幂等 signal 更新；冷启动有先验收缩；查询结果可解释；调度消费信誉且保留探索奖励 |
| M9C Metering | 租约 wall-clock、Artifact 字节、Verifier 调用、唤醒次数、Agent 自报 token 分账 | 平台权威与自报口径明确区分；记录有幂等键、价格表版本和 mission/agent/subtask 归属；可生成 Mission 用量报告 |
| M9D Observability | Prometheus 指标、运行快照、HTTP RED 指标、W3C trace 上下文 | `/metrics` 可抓取；快照展示任务状态、Agent 健康和待决策数；每个响应回传 `traceparent`/`X-Trace-ID` |
| M9E 接入与回归 | REST、Python SDK、内存/PG/e2e 测试与文档 | `go test ./...`、race、e2e、Python SDK 与真实 PostgreSQL 迁移回放通过 |

## 2. API 契约

- `POST /v1/artifacts/{id}/verify`：提交一次最终质量记录。服务端执行 Artifact
  哈希/大小 L0 校验，并验证调用者不是生产 Agent；请求可携带 L1–L3 证据。
- `GET /v1/artifacts/{id}/quality`：读取质量记录。
- `GET /v1/agents/{id}/reputation`：读取按 skill 展开的信誉及综合分。
- `GET /v1/missions/{id}/usage`：读取计量明细和按资源/信任级汇总。
- `GET /v1/observability/snapshot`：控制面运行快照。
- `GET /metrics`：Prometheus text exposition。

鉴权启用时，质量写入仅允许 `human/service`，或具备自己稳定身份且不等于生产者的
Agent；质量、信誉、用量查询允许 `human/service`，Agent 仅可读取自己的信誉。

## 3. 数据与算法

### 3.1 质量记录

每个 Artifact 只有一个最终记录，主键同时承担幂等约束。分数和置信度均在 `[0,1]`；
verdict 为 `accepted/rework/rejected`；失败分类限定为
`schema_invalid/fact_conflict/incomplete/style/policy_violation/judge_rejected`。
记录保存每层结果、Verifier 身份、rubric、上下文 hash 和时间戳。

### 3.2 信誉

- 成功率和可靠性使用 Beta(2,2) 先验；
- 质量、时延、成本效率使用带单次变化限幅的 EWMA；
- 未验证完成/失败只以 `0.25` 权重进入，Verifier 结果以 `1.0` 权重进入；
- signal ID 唯一，清扫器可安全重放修复跨事务中断；
- capability-first 得分加入信誉综合分，并给低样本 Agent 有界探索奖励。

### 3.3 计量

记录字段包括 resource、quantity、unit、trust、price-book、credits 以及 mission/subtask/
agent 归属。`lease.wall_ms/artifact.byte/verify.call/wake.fire` 为平台权威口径；
`token.reported` 为自报口径，默认不产生可结算 credit。

## 4. 一致性与恢复

- 质量记录、信誉 signal、信誉投影和 `artifact.verified` 事件在同一存储事务提交；
- 任务终态与信誉 signal 采用幂等最终一致：请求路径即时更新，sweeper 重放补偿；
- 计量记录使用稳定 ID，重复执行不会重复计费；
- PostgreSQL 迁移只前向新增表/索引，不改写既有历史。

## 5. 明确边界

- L2 模型推理由外部 Verifier Agent/服务执行；控制面负责隔离、契约、留痕与聚合，不托管模型。
- 本里程碑不接真实支付；自报 token 未经网关仲裁只用于审计。
- OpenTelemetry Collector/Grafana/Tempo 是部署集成项；本里程碑输出兼容的 trace/metrics 信号。
- Marketplace、申诉工作流、金丝雀样本库与完整 React Console 作为 M10 产品化范围。
