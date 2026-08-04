# M4 实现计划：event/condition 唤醒 + TaskIntent 准入管道

> 对应设计文档 §7（触发体系）、§14.2（事件模式匹配）、§14.3（条件表达式）、§14.4（唤醒语义）。
> 流程约定同前：本计划先行；每个切片 `go test ./...` 全绿才标记完成。
> 前置：M3 已交付 WAITING 挂起-唤醒骨架（timer/manual + TTL + CAS 恰好一次 + 检查点续跑）。

## 1. 范围（In Scope）

| # | 切片 | 内容 | 设计章节 |
|---|------|------|---------|
| G1 | event 唤醒 | `wake_on.kind=event`：`event.types` 单事件类型过滤 + `event.where` 载荷等值谓词（子集匹配）；**水位线语义**：suspend 时记录 `after_seq`（当前最大事件 seq），只匹配注册后到达的事件（edge-fired 无歧义、重启安全）；sweeper 增量评估 + CAS 唤醒 | §14.2 规则 1、§14.4 |
| G2 | condition 唤醒 | `wake_on.kind=condition`：结构化谓词 `{board: "ns/key", op: exists\|equals, value}`；**求值器接口** `ConditionEvaluator` 预留 CEL 内核槽位；BoardPut 成功后增量评估 + sweeper 全量兜底（anti-entropy，§14.3 求值策略的 MVP 形态） | §14.3, §14.4 |
| G3 | TaskIntent 准入管道 | `POST /v1/intents`：归一化人/Agent 触发入口；`action=create_mission`（幂等键去重，重复返回原 mission + `deduplicated:true`）与 `action=wake`（唤醒 WAITING 子任务）；source（kind/id）落事件留痕 | §7.1, §7.2 |

## 2. 明确不做（M4 边界）

- **CEL 表达式内核**：设计 §14.3 选型 CEL 不变；M4 先用结构化谓词打通"注册 → 增量评估 → CAS 唤醒"全链路，`ConditionEvaluator` 接口即替换槽位（M5 嵌入 cel-go，含 cost 上限与静态引用提取）；
- **复合事件模式（CEP）**：`all/sequence/not`、窗口/watermark/关联键（§14.2 规则 2-4）——M4 仅单事件模式；
- **倒排索引**：MVP 全量评估（WAITING 注册量级小），索引优化随 CEL 内核一起做；
- **scope 鉴权强制**（§7.2 三级授权）：intents 记录 source 但不鉴权（同 M2/M3 无认证定位）；
- **A2A/MCP 适配、OpenClaw/Hermes 托管 Adapter**：移至 M5；
- 跨 Mission 事件等待：M4 event 唤醒仅限同 Mission 事件流（事件表按 mission 索引，跨域匹配随事件总线外迁再做）。

## 3. 关键语义决策

1. **水位线（after_seq）而非时间戳**：suspend 注册 event 等待时记录当前事件最大 seq，只匹配其后的新事件——时间戳比较在时钟回拨/同毫秒并发下有歧义，seq 单调且重启安全（进程无需持水位线状态）；
2. **恰好一次沿用 M3 机制**：CAS WAITING→READY（校验 version）为多 sweeper/增量+兜底双路竞争的唯一仲裁；唤醒即清空注册（一次性），level-triggered 再等待须重新 suspend；
3. **where 谓词为子集等值匹配**：`{"payload.issue_id": "123"}` 点路径下钻，全部键等值才算命中——等值判断用 JSON 规范化比较，不做类型强转；
4. **condition 只对 board 求值**：`exists`（键存在即真）与 `equals`（值 JSON 等值）；黑板值本身是 JSON 文档时按整体比较（不下钻文档内部，CEL 内核引入后支持路径表达式）；
5. **Intent 幂等独立命名空间**：复用 idempotency_keys 表，键加 `intent-` 前缀与子任务完成键隔离；重复提交返回 200 + 原 mission_id + `deduplicated:true`（与 complete 幂等语义一致）；
6. **event/condition 唤醒同属 WAITING**：TTL（wake_deadline）必填不变，过期 FAILED(wake_timeout) + 级联取消——M3 语义对所有 wake kind 统一成立。

## 4. 模型与存储变更

- `mission.WakeSpec` 上移为完整注册结构（kind/at/deadline/event/condition），`Subtask` 增 `WakeSpec json.RawMessage` 字段（kind/at/deadline 仍冗余为顶层列供索引查询）；
- 迁移 `0004_m4.sql`：subtasks 增列 `wake_spec jsonb`；
- Store 接口调整/新增：
  - `SuspendSubtask` 签名改为接收完整 `*mission.WakeSpec`；
  - `ListWaiting(ctx, kind)` 按唤醒类型扫描（event/condition 评估用）；
  - `MaxEventSeq(ctx)`（suspend 注册水位线）；
  - `PutIdempotent(ctx, key, result)`（intents 幂等，复用 idempotency_keys 表）。

## 5. 测试方案

- core：event 唤醒（注册前旧事件不命中 / 注册后匹配事件唤醒 / where 谓词过滤 / 其他 Mission 事件隔离）；condition 唤醒（BoardPut 命中即醒 / equals 不等不醒 / sweep 兜底）；TTL 对 event/condition 同样生效；intent create_mission 幂等（重发返回同 mission）；intent wake；
- store（memory）：水位线注册与读取、ListWaiting 过滤、PutIdempotent 冲突；
- e2e（`-tags e2e`）：两任务流水线——任务 B suspend 等"任务 A 完成"事件，A 完成后 sweeper 唤醒 B；Agent 经 /v1/intents 提交幂等 Mission。

**DoD**：`go test ./...` + `go test -tags e2e ./e2e/` 全绿；README 补充 event/condition 唤醒与 intents API 示例。
