package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"redismq/internal/config"
	"redismq/internal/logger"
	"redismq/internal/model"
	"redismq/internal/redisx"

	"github.com/redis/go-redis/v9"
)

func main() {
	logger.InitLogger("dlq-replay")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := config.LoadConsumerConfig()
	rdb := redisx.NewClient(cfg.Redis)
	if err := redisx.WaitForRedis(ctx, rdb); err != nil {
		slog.Error("redis connection initialization failed", "error", err)
		os.Exit(1)
	}

	count := int64(getEnvInt("DLQ_REPLAY_COUNT", 100))
	keep := getEnvBool("DLQ_REPLAY_KEEP_SOURCE", false)

	replayed, failed, err := replayDLQ(ctx, rdb, cfg.DLQStream, cfg.StreamName, count, keep)
	if err != nil {
		slog.Error("dlq replay failed", "error", err, "replayed", replayed, "failed", failed)
		os.Exit(1)
	}

	slog.Info("dlq replay completed", "replayed", replayed, "failed", failed, "keep_source", keep)
}

func replayDLQ(ctx context.Context, rdb *redis.Client, dlqStream, targetStream string, count int64, keepSource bool) (int, int, error) {
	messages, err := rdb.XRangeN(ctx, dlqStream, "-", "+", count).Result()
	if err != nil {
		return 0, 0, err
	}

	replayed := 0
	failed := 0

	for _, msg := range messages {
		event, parseErr := model.MustEventFromDLQ(msg)
		if parseErr != nil {
			failed++
			slog.Error("skip invalid dlq entry", "dlq_id", msg.ID, "error", parseErr)
			continue
		}

		values := event.StreamValues()
		values["replayed_from_dlq"] = "true"
		values["dlq_id"] = msg.ID
		values["replayed_at"] = time.Now().UTC().Format(time.RFC3339Nano)

		if _, addErr := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: targetStream,
			Values: values,
		}).Result(); addErr != nil {
			failed++
			slog.Error("requeue dlq entry failed", "dlq_id", msg.ID, "event_id", event.EventID, "error", addErr)
			continue
		}

		if !keepSource {
			if delCount, delErr := rdb.XDel(ctx, dlqStream, msg.ID).Result(); delErr != nil {
				failed++
				slog.Error("delete dlq entry after replay failed", "dlq_id", msg.ID, "event_id", event.EventID, "error", delErr)
				continue
			} else if delCount == 0 {
				failed++
				slog.Error("delete dlq entry after replay affected zero entries", "dlq_id", msg.ID, "event_id", event.EventID)
				continue
			}
		}

		replayed++
		slog.Info("replayed dlq entry", "dlq_id", msg.ID, "event_id", event.EventID)
	}

	if failed > 0 {
		return replayed, failed, errors.New("dlq replay completed with failures")
	}

	return replayed, failed, nil
}

func getEnvInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func getEnvBool(key string, fallback bool) bool {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	switch raw {
	case "1", "true", "TRUE", "yes", "YES":
		return true
	case "0", "false", "FALSE", "no", "NO":
		return false
	default:
		return fallback
	}
}
