# Console

M10 React/Vite 运维台，覆盖 Overview、Marketplace、Simulation Lab、Quality Appeals、Mission Inspector 与 Usage。

```bash
npm install
npm run dev
```

`VITE_TROOP_API` 可指定控制平面地址，默认同源。`troopd` 根路径仍内置零构建 Mission/SSE 控制台，适合单二进制部署；本目录可独立构建并交给 CDN/Ingress 托管。
