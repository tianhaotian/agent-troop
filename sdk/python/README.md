# Agent Troop Python SDK

零第三方依赖的 Agent/业务客户端，封装 Bearer、注册、offer、租约、fencing、心跳、
幂等完成和 Artifact 签名 URL。

```python
from agent_troop import AgentWorker, Client

client = Client("http://localhost:8080", token="...")
worker = AgentWorker(client, "python-1")

def run(context_package):
    return {"result_ref": "artifact://answer", "usage_tokens": 1200}

while True:
    worker.run_once(run)
```

开发安装与测试：

```bash
python3 -m pip install -e sdk/python
python3 -m unittest discover -s sdk/python/tests
```
