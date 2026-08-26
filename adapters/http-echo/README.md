# http-echo Adapter（M1 参考实现）

通用 HTTP Adapter 参考实现：按 §9 执行面协议接收子任务（lease accept → progress → complete），
回显输入作为产物输出。用途：

1. 端到端自测（与模拟 Agent 接力完成 Mission，M1 验收）；
2. 第三方平台（OpenClaw / Hermes）接入时的协议兼容性基准（设计 §6.3、§22.9）。

已实现注册、offer 轮询、accept/start/complete 接力。需要外部 runtime、Bearer 和长任务
心跳时使用 [managed-http](../managed-http/README.md)。

认证开启时设置 `TROOP_TOKEN` 与可选 `TROOP_AUTH_SUBJECT`；Agent 需要先由 privileged
身份注册，或在首次注册时使用 service token。
