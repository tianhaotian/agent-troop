# M8 实现计划：安全外部接入与协议生态

> 前置：M1–M7 已完成控制面、主子委托、恢复、预算和最小知情上下文。
> M8 不改变内部任务/租约语义，只在边界层补齐身份、资源分发与标准协议翻译。
> 协议基线：[A2A 1.0](https://a2a-protocol.org/latest/specification/) 与
> [MCP 2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28)。

## 1. 范围与验收

| 切片 | 范围 | 验收 |
|---|---|---|
| M8A | HMAC-SHA256 Bearer 身份令牌；human/agent/service 三元身份；Agent 外部 subject 与注册表绑定；路由级角色和 Agent 自身约束 | 开启认证后匿名访问受保护 API 返回 401，跨 Agent 冒用返回 403，过期/篡改 token 被拒；未配置密钥保持本地开发兼容 |
| M8B | Artifact 短时签名下载 URL；Agent 必须以活跃 lease 且 artifact 位于其不可变 context package 中申请；human/service 可按审计身份申请 | URL 过期、篡改、跨 artifact 重放均失败；签名下载不需要暴露 Bearer token |
| M8C | A2A 1.0 Agent Card + JSON-RPC 边界；MCP 2026-07-28 Streamable HTTP 的 discover/tools/resources 最小服务器；全部映射既有 Service | A2A 可创建/查询/取消 Mission；MCP 可列举并调用 Mission 工具、读取 Mission 资源；错误符合 JSON-RPC 2.0 |
| M8D | 通用托管 HTTP Adapter 与 Python SDK；Bearer token、租约、心跳、fencing、幂等封装 | 外部 runtime 只实现单个 run HTTP 接口即可完成 offer→accept→start→complete；SDK 有离线单测 |
| M8E | 文档、迁移、协议/安全单测和 E2E，PG 迁移重放 | `go test ./...`、`go vet ./...`、race、e2e、PG CI 全绿 |

## 2. 身份与兼容边界

- `TROOP_AUTH_SECRET` 非空时启用认证；空值仅用于本地开发和历史测试。
- token 为平台签发的紧凑 HMAC 令牌，claims 包含 `sub/kind/scopes/exp`；生产由外部
  OIDC/API Gateway 换取平台 token，控制面不保存上游会话或密码。
- Agent 注册项可声明唯一 `auth_subject`。启用认证时 Agent token 的 `sub` 必须与之匹配；
  未声明时兼容地回退为 Agent ID。请求体中的 `agent_id` 不再是身份凭据。
- human/service 可管理 Mission、Agent、Decision 和 Board；agent 仅能操作自己的
  heartbeat/offers/lease/subtask，并继续受 fencing、trigger scope 和权限包络约束。
- 健康探针、Console、Agent Card 公开；Artifact 签名下载由 URL 自身授权。

## 3. 协议映射

- A2A 1.0 使用 JSON-RPC 2.0：`SendMessage` 创建单任务 Mission，`GetTask` 查询，
  `CancelTask` 取消；同时兼容 0.x 方法名。Agent Card 暴露能力与认证要求，A2A task id 等于 Mission id。
- MCP 2026-07-28 提供 `server/discover`、`initialize`、`tools/list`、`tools/call`、`resources/list`、
  `resources/templates/list`、`resources/read`，并兼容 2025 客户端。工具映射 create/get/cancel/wake，资源 URI
  使用 `troop://missions/{id}`。
- 外部协议错误只在边界翻译；Store 与事件类型保持稳定，actor 取认证身份。

## 4. 明确不做

- 内置完整 OIDC Authorization Code 流程或保存用户密码；由网关/IdP 完成登录与 token exchange。
- A2A push notification、长连接流式消息与 MCP sampling/elicitation；待真实接入方提出需求后扩展。
- OpenClaw/Hermes 私有 API 的硬编码客户端；M8D 的托管 Adapter 用可配置 HTTP runtime
  契约覆盖两者，平台专属认证放 Adapter 侧。
- Verifier、信誉反馈、Marketplace、真实支付和计费争议仲裁；进入后续质量阶段。

## 5. DoD

- 安全逻辑使用常量时间签名比较、明确过期时间和注入逻辑时钟；不记录明文 secret/token。
- 所有新增外部入口有 body 限制、结构校验、身份约束、幂等或只读语义。
- 更新 README 路线图、快速开始、SDK/Adapter 文档；提交前运行完整测试门槛。
