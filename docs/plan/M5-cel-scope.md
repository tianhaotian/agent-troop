# M5 实现计划：CEL 条件内核 + 触发 scope 授权

> 对应设计文档 §14.3（CEL 选型与安全护栏）、§7.2①（准入鉴权）、§7.4（能力分级授权）。
> 流程约定同前：本计划先行；每个切片 `go test ./...` 全绿才标记完成。
> 前置：M4 已交付结构化谓词 condition（exists/equals）与 `ConditionEvaluator` 替换槽位、intents 记录 source 未鉴权。
> 范围决策（2026-08-06 与用户确认）：M5 只做内核与治理两项；A2A/MCP 适配、OpenClaw/Hermes 托管 Adapter、Agent 生态接入移至 M6。

## 1. 范围（In Scope）

| # | 切片 | 内容 | 设计章节 |
|---|------|------|---------|
| H1 | CEL 条件内核 | `wake_on.condition.expr` 嵌入 cel-go：注册时编译 + 类型检查 + 静态 cost 估算拒超；运行时 cost 上限（超限视为 false 并留痕）；数据模型 `board.<ns>.<key>` / `mission.*` / `subtask.*` / `elapsed()` / `deadline_in()`（逻辑时钟注入，不暴露裸时钟）；**静态引用提取**（board ns/key 键集）供 BoardPut 增量评估过滤；结构化谓词（M4）保留兼容，双路径共存 | §14.3 |
| H2 | scope 三级授权 | agents 增 `trigger_scopes`；intents 管道对 `source.kind=agent` 强制校验：`create_mission` 需 `trigger.create_mission`、`wake` 需 `trigger.wake`；human source 不鉴权（同 M2-M4 无认证定位）；未注册 Agent 拒绝 | §7.2①, §7.4(1) |

## 2. 明确不做（M5 边界）

- **CEL 数据模型的 artifacts/agents 视图**（§14.3 完整清单中的两项）：M5 只暴露 board/mission/subtask 与两个时钟函数；产物元数据视图随 Artifact 查询接口补齐后再开；
- **复合 CEL 注册结构**（`any:`/`all:` 组合，§14.3 示例形态）：M5 单条 expr（逻辑与或可在表达式内写），组合结构随 CEP（§14.2 规则 2-4）一起做；
- **独立倒排索引表**：引用键集随 wake_spec 持久化、增量评估按键集过滤（M4 同款模式的推广）；WAITING 量级小，独立索引结构等量级上来再建；
- **配额/预算校验、循环检测、限流**（§7.2 ③④⑤）：预算池与触发因果图体系未建，M6+；
- **`trigger.spawn_subtask`**（§7.4 第一个 scope）：依赖主子委托协议（§15），M6 与 delegate 动作一起交付；M5 只常量预留；
- **human 触发者 SSO/RBAC、scope 授予的治理面**（谁能授 scope）：全库无认证定位不变，注册时自声明并事件留痕；
- **A2A/MCP 适配、OpenClaw/Hermes 托管 Adapter**：M6。

## 3. 关键语义决策

1. **expr 与结构化谓词互斥**：`condition` 二选一——`{board, op, value}`（M4 路径，求值器不变）或 `{expr}`（CEL 路径）；同现或同缺均在 suspend 校验拒绝。旧注册无 expr，行为逐字节不变；
2. **board 映射为嵌套 map**：`board.<ns>.<key>` → `map[string]dyn`（ns → key → JSON 解码值）；点路径与 `board["ns"]["key"]` 等价（CEL 对 map 的 select 语义）。ns/key 含点号时须用下标写法（文档注明）；
3. **时钟函数只读逻辑时钟**：`elapsed()` = 自 suspend 注册起、`deadline_in()` = 距 wake deadline，返回 CEL duration；由注入 clock 计算，表达式内无裸系统时钟/随机源（§14.3 护栏，与工程红线一致）；
4. **cost 双闸**：注册时 cel-go 静态 cost 估算超上限直接拒绝（400）；运行时 `OptTrackCost + CostLimit` 中断按 false 处理并落 `condition.cost_exceeded` 事件留痕（§14.3"超限视为 false 并告警"）；
5. **引用提取失败即通配**：AST 遍历提取 `board.*` 静态键集；遇到动态下标（`board[x]` 变量）等不可静态判定的形态，该注册标记 `refs=*`，增量评估永远包含它（宁多评不漏评，sweeper 兜底语义不变）；
6. **增量过滤复用 M4 钩子**：BoardPut 成功后将 changed key 与注册引用键集比对，不交集跳过；表达式含 `mission.*`/`subtask.*`/时钟函数的注册在 BoardPut 增量中同样跳过（黑板写入不改变这些输入），sweeper 全量兜底负责它们（level-triggered 语义由兜底保证，§14.4）；
7. **scope 默认收紧**：`trigger_scopes` 注册时显式声明，缺省 `[]`——即 Agent 默认不能 create_mission/wake（§7.4"默认收紧、按授权放开"）；human source 不校验（无认证体系，越权防护等 SSO/RBAC）；scope 校验失败返回 403 且不落幂等键（未授权请求不消耗去重窗口）；
8. **鉴权在去重之前**：管道顺序对齐 §7.2（①鉴权 → ②去重 → ⑥实例化），未授权重复提交不得经 `deduplicated:true` 侧漏 Mission 存在性。

## 4. 模型与存储变更

- `mission.BoardCondition` 增 `Expr string` 与（求值缓存用，不持久化单独列）编译产物；wake_spec 内 condition 增加 `expr` 字段（jsonb 内演进，**无需改表**）；
- 迁移 `0005_m5.sql`：`agents ADD COLUMN trigger_scopes jsonb NOT NULL DEFAULT '[]'`；
- Store 接口：`RegisterAgent`/`GetAgent` 透传 `TriggerScopes []string`（pg + memory 双实现）；
- core 新增 `internal/core/cel.go`：CEL 环境单例（声明 board/mission/subtask/elapsed/deadline_in）、编译+引用提取、求值（含 cost 中断）；
- API：`POST /v1/agents/register` 接受 `trigger_scopes`；intents 错误语义新增 403。

## 5. 测试方案

- core（CEL）：expr 编译错误拒注册；`board.shared.count >= 2` 型表达式 BoardPut 命中唤醒 / 未命中不醒；`equals` 旧谓词回归；动态下标注册（`refs=*`）任意 BoardPut 都评估；cost 超限表达式拒注册（静态）与运行超限按 false（构造深嵌套/大列表字面量）；`deadline_in() < duration("0s")` 由 sweeper 兜底唤醒；跨 Mission 黑板隔离；
- core（scope）：无 scope Agent create_mission → 403 且不消耗幂等键；授权后成功；wake scope 独立判定；未注册 Agent 拒绝；human source 直通；
- store（memory/pg）：trigger_scopes 存取回归；
- e2e（`-tags e2e`）：CEL 条件流水线（B 挂起等 `board.shared.input_ready == true`，A BoardPut 后 B 醒）；未授权 Agent intent 403 → 注册带 scope 后幂等提交成功。

**DoD**：`go test ./...` + `go test -tags e2e ./e2e/` 全绿；README 补 CEL expr 示例与 scope 授权说明；迁移清单补 0005。
