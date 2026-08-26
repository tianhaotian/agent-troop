# Managed HTTP Adapter

把 Agent Troop 的注册、offer、租约、心跳、fencing 和结果幂等协议翻译成一个最小
runtime HTTP 调用。OpenClaw、Hermes 或自研运行时只需暴露：

```http
POST /run
Authorization: Bearer $RUNTIME_TOKEN
Content-Type: application/json

{"task": {...}, "context_package": {...}}
```

成功响应：

```json
{"result_ref":"artifact://result","usage_tokens":1200}
```

启动：

```bash
TROOP_TOKEN=... RUNTIME_TOKEN=... go run ./adapters/managed-http \
  -id openclaw-1 -platform openclaw -runtime http://localhost:9090 \
  -skills web.research,document.write
```

认证开启时，Agent 必须先由 human/service 身份注册，且 `auth_subject` 与
`TROOP_TOKEN` 的 subject 相同；Adapter 遇到注册 403 后会使用已配置身份继续运行。
