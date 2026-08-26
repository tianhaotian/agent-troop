package api

// 极简路由器：Go 1.21 的 http.ServeMux 不支持方法匹配与 {param} 通配
//（1.22 才引入），这里用约 60 行实现本项目所需子集：
// "GET /v1/missions/{id}" 形式注册，参数经 request context 传递（pv 读取）。
import (
	"context"
	"net/http"
	"strings"
)

type pathKey string

const pathValuesKey pathKey = "troop.path_values"

// pv 读取路径参数（等价 1.22 的 r.PathValue）。
func pv(r *http.Request, name string) string {
	vals, _ := r.Context().Value(pathValuesKey).(map[string]string)
	return vals[name]
}

type route struct {
	method string
	segs   []string
	h      http.HandlerFunc
}

type router struct {
	routes []route
}

func newRouter() *router { return &router{} }

// handle 注册 "METHOD /path/{param}/..." 形式的路由。
func (rt *router) handle(pattern string, h http.HandlerFunc) {
	parts := strings.SplitN(pattern, " ", 2)
	if len(parts) != 2 {
		panic("api: bad route pattern " + pattern)
	}
	path := strings.Trim(parts[1], "/")
	var segs []string
	if path != "" {
		segs = strings.Split(path, "/")
	}
	rt.routes = append(rt.routes, route{method: parts[0], segs: segs, h: h})
}

func (rt *router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.Trim(r.URL.Path, "/")
	var segs []string
	if path != "" {
		segs = strings.Split(path, "/")
	}
	for _, ro := range rt.routes {
		if ro.method != r.Method || len(ro.segs) != len(segs) {
			continue
		}
		vals := map[string]string{}
		ok := true
		for i, seg := range ro.segs {
			if strings.HasPrefix(seg, "{") && strings.HasSuffix(seg, "}") {
				vals[seg[1:len(seg)-1]] = segs[i]
			} else if seg != segs[i] {
				ok = false
				break
			}
		}
		if ok {
			ctx := context.WithValue(r.Context(), pathValuesKey, vals)
			ro.h(w, r.WithContext(ctx))
			return
		}
	}
	writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
}
