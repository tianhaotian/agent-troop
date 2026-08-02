package api

import "net/http"

// 最小只读 Console（M1 切片 S11）：输入 Mission ID 后展示子任务状态与
// SSE 事件时间线。正式 React Console 见 web/console/（M2）。
const consoleHTML = `<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>Agent Troop Console (M1)</title>
<style>
  body { font-family: ui-monospace, Menlo, monospace; max-width: 960px; margin: 2rem auto; padding: 0 1rem; }
  input { padding: .4rem; width: 22rem; }
  button { padding: .4rem .8rem; }
  #subs span { display: inline-block; margin: .2rem; padding: .3rem .6rem; border-radius: 4px; background: #eee; }
  .READY { background: #ffd !important; } .OFFERED,.LEASED,.RUNNING { background: #bde !important; }
  .SUCCEEDED { background: #cfc !important; } .FAILED { background: #fcc !important; }
  .CANCELLED { background: #ddd !important; text-decoration: line-through; }
  #events { border-top: 1px solid #ccc; margin-top: 1rem; font-size: .85rem; }
  #events div { padding: .15rem 0; border-bottom: 1px dashed #eee; }
</style>
</head>
<body>
<h1>Agent Troop <small>M1 Console</small></h1>
<div>
  <input id="mid" placeholder="Mission ID (msn_...)">
  <button onclick="watch()">Watch</button>
  <span id="mstatus"></span>
</div>
<h3>Subtasks</h3>
<div id="subs"></div>
<h3>Events (SSE)</h3>
<div id="events"></div>
<script>
let es = null;
async function watch() {
  const id = document.getElementById('mid').value.trim();
  if (!id) return;
  if (es) es.close();
  document.getElementById('events').innerHTML = '';
  await refresh(id);
  es = new EventSource('/v1/missions/' + id + '/events');
  es.onmessage = e => {
    const ev = JSON.parse(e.data);
    const d = document.createElement('div');
    d.textContent = '#' + ev.seq + ' ' + ev.type + ' ' + JSON.stringify(ev.payload || {});
    document.getElementById('events').appendChild(d);
    refresh(id);
  };
}
async function refresh(id) {
  const r = await fetch('/v1/missions/' + id);
  if (!r.ok) return;
  const body = await r.json();
  document.getElementById('mstatus').textContent = ' [' + body.mission.status + ']';
  const el = document.getElementById('subs');
  el.innerHTML = '';
  for (const s of body.subtasks) {
    const sp = document.createElement('span');
    sp.className = s.state;
    sp.textContent = s.id.split('_').pop() + ': ' + s.state + (s.assignee_agent_id ? ' → ' + s.assignee_agent_id : '');
    el.appendChild(sp);
  }
}
</script>
</body>
</html>`

func (s *Server) console(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(consoleHTML))
}
