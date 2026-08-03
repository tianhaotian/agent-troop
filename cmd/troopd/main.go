// troopd：Agent Troop 控制平面单二进制。
// 存储：TROOP_PG_DSN 设置时用 PostgreSQL（docker compose up -d postgres），
// 否则回退内存存储（本地零依赖体验；数据不持久）。
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"agenttroop/internal/api"
	"agenttroop/internal/clock"
	"agenttroop/internal/core"
	"agenttroop/internal/store"
	"agenttroop/internal/store/memory"
	"agenttroop/internal/store/pg"
)

func main() {
	clk := clock.RealClock{} // ADR-8：时钟注入
	ctx := context.Background()

	var st store.Store
	if dsn := os.Getenv("TROOP_PG_DSN"); dsn != "" {
		pgStore, err := pg.Connect(ctx, dsn)
		if err != nil {
			log.Fatalf("connect pg: %v", err)
		}
		defer pgStore.Close()
		st = pgStore
		log.Printf("store: postgresql")
	} else {
		st = memory.New()
		log.Printf("store: in-memory (set TROOP_PG_DSN for persistence)")
	}

	svc := core.New(st, clk, core.DefaultConfig()).
		WithBlob(core.FSBlob{Dir: envOr("TROOP_BLOB_DIR", "./data/artifacts")})
	handler := api.New(svc).Handler()

	// 后台循环：调度器 + human 节点工单 + 清扫器（轮询间隔为运行机制，非业务时间语义）
	stop := make(chan struct{})
	go loop(stop, 500*time.Millisecond, func() {
		if _, err := svc.ScheduleOnce(ctx); err != nil {
			log.Printf("schedule: %v", err)
		}
		if _, err := svc.OpenHumanDecisions(ctx); err != nil {
			log.Printf("open human decisions: %v", err)
		}
	})
	go loop(stop, 5*time.Second, func() {
		if err := svc.SweepOnce(ctx); err != nil {
			log.Printf("sweep: %v", err)
		}
	})

	addr := envOr("TROOPD_ADDR", ":8080")
	srv := &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("troopd listening on %s (console: http://localhost%s/)", addr, addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	close(stop)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

func loop(stop <-chan struct{}, interval time.Duration, fn func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			fn()
		}
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
