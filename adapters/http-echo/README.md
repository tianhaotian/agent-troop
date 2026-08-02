# http-echo Adapter（M1 切片 S10，待实现）

通用 HTTP Adapter 参考实现：按 §9 执行面协议接收子任务（lease accept → progress → complete），
回显输入作为产物输出。用途：

1. 端到端自测（与模拟 Agent 接力完成 Mission，M1 验收）；
2. 第三方平台（OpenClaw / Hermes）接入时的协议兼容性基准（设计 §6.3、§22.9）。

实现计划见 [docs/plan/M1-mvp.md](../../docs/plan/M1-mvp.md)。
