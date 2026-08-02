// troopd：Agent Troop 控制平面单二进制（M1：API 骨架，后续切片逐步挂载
// Orchestrator / Scheduler / Registry，见 docs/plan/M1-mvp.md）。
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"agenttroop/internal/clock"
)

func main() {
	clk := clock.RealClock{} // ADR-8：时钟注入，禁止散落 time.Now()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "ok",
			"ts":     clk.Now().UTC().Format(time.RFC3339Nano),
		})
	})

	// S8 占位：任务面 API 在后续切片实现（docs/plan/M1-mvp.md）
	mux.HandleFunc("POST /v1/missions", notImplemented)
	mux.HandleFunc("GET /v1/missions/{id}", notImplemented)

	addr := envOr("TROOPD_ADDR", ":8080")
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		log.Printf("troopd listening on %s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}

func notImplemented(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusNotImplemented, map[string]string{
		"error": "not implemented in M1 slice S1; see docs/plan/M1-mvp.md",
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
