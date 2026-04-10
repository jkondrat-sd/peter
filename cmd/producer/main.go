package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"redismq/internal/config"
	"redismq/internal/logger"
	"redismq/internal/redisx"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const StreamName = "event_queue"

func main() {
	logger.InitLogger("producer")
	slog.Info("Service Initialization: Producer Component Started")

	cfg := config.LoadRedisConfig()
	rdb := redisx.NewClient(cfg)

	// 绑定操作系统终端信号至 Context，实现进程的平滑和安全退出
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := redisx.WaitForRedis(ctx, rdb); err != nil {
		slog.Error("Redis connection initialization failed", "error", err)
		os.Exit(1)
	}

	script := redis.NewScript(redisx.ProducerLuaScript)
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	// 基于 Context 驱动的生产主循环
	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutdown signal received. Terminating producer loops.")
			return
		case <-ticker.C:
			produceDataEvent(ctx, rdb, script)
		}
	}
}

// produceDataEvent 封装消息生产的单次调用逻辑，依据 Context 进行链路级别调度控制
func produceDataEvent(ctx context.Context, rdb *redis.Client, script *redis.Script) {
	sessionID := uuid.New().String()
	cacheKey := fmt.Sprintf("session:%s", sessionID)
	sessionPayload := `{"user": "alice", "status": "active"}`
	ttl := 300 
	streamPayload := fmt.Sprintf("STREAM_EVENT:%s", sessionID)

	keys := []string{cacheKey, StreamName}
	args := []interface{}{sessionPayload, ttl, streamPayload}

	// 触发执行底层的 Lua 事务脚本，保证两阶段提交在此过程中的绝对状态一致性
	result, err := script.Run(ctx, rdb, keys, args...).Result()
	if err != nil {
		slog.Error("Failed to execute atomic operation", "error", err)
		return
	}
	slog.Info("Atomic publish transaction completed successfully", "cache_key", cacheKey, "stream_id", result)
}
