package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"redismq/internal/config"
	"redismq/internal/logger"
	"redismq/internal/metrics"
	"redismq/internal/redisx"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger.InitLogger("producer")
	slog.Info("producer started")

	cfg := config.LoadProducerConfig()
	rdb := redisx.NewClient(cfg.Redis)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	metrics.StartServer(ctx, cfg.MetricsAddr)

	if err := redisx.WaitForRedis(ctx, rdb); err != nil {
		slog.Error("Redis connection initialization failed", "error", err)
		os.Exit(1)
	}

	script := redis.NewScript(redisx.ProducerLuaScript)

	slog.Info("producer workers online", "workers", cfg.Workers, "stream", cfg.StreamName, "interval", cfg.Interval.String())

	for i := 0; i < cfg.Workers; i++ {
		go func(workerID int) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					produceDataEvent(ctx, rdb, script, cfg)
					time.Sleep(cfg.Interval)
				}
			}
		}(i)
	}

	<-ctx.Done()
	slog.Info("producer shutting down")
}

func produceDataEvent(ctx context.Context, rdb *redis.Client, script *redis.Script, cfg config.ProducerConfig) {
	eventID := uuid.NewString()
	sessionID := uuid.New().String()
	dedupeKey := fmt.Sprintf("dedupe:event:%s", eventID)
	producedAt := time.Now().UTC()

	payloadBytes, err := json.Marshal(map[string]string{
		"event_id":   eventID,
		"session_id": sessionID,
		"user":       "alice",
		"status":     "active",
	})
	if err != nil {
		slog.Error("failed to marshal payload", "error", err)
		return
	}

	keys := []string{dedupeKey, cfg.StreamName}
	args := []interface{}{
		eventID,
		int(cfg.DedupeTTL / time.Second),
		sessionID,
		string(payloadBytes),
		producedAt.Format(time.RFC3339Nano),
	}

	result, err := script.Run(ctx, rdb, keys, args...).Result()
	if err != nil {
		metrics.ProducerPublishFailedTotal.Inc()
		slog.Error("failed to publish event", "event_id", eventID, "error", err)
		return
	}

	if streamID, ok := result.(string); ok && streamID != "" {
		metrics.ProducerPublishedTotal.Inc()
		slog.Info("event published", "event_id", eventID, "stream_id", streamID)
		return
	}

	metrics.ProducerDuplicateSuppressedTotal.Inc()
	slog.Warn("duplicate event suppressed", "event_id", eventID)
}
