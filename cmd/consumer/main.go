package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"redismq/internal/config"
	"redismq/internal/consumer"
	"redismq/internal/dbx"
	"redismq/internal/logger"
	"redismq/internal/metrics"
	"redismq/internal/redisx"
)

func main() {
	cfg := config.LoadConsumerConfig()
	logger.InitLogger(cfg.ConsumerName)
	slog.Info("consumer started", "consumer", cfg.ConsumerName, "stream", cfg.StreamName, "group", cfg.GroupName)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	metrics.StartServer(ctx, cfg.MetricsAddr)

	rdb := redisx.NewClient(cfg.Redis)
	if err := redisx.WaitForRedis(ctx, rdb); err != nil {
		slog.Error("Redis connection initialization failed", "error", err)
		os.Exit(1)
	}

	dbCluster, err := dbx.InitDBCluster(ctx, cfg.DB)
	if err != nil {
		slog.Error("MySQL cluster initialization failed", "error", err)
		os.Exit(1)
	}

	service := consumer.NewService(cfg, rdb, dbCluster)
	if err := service.Run(ctx); err != nil && err != context.Canceled {
		slog.Error("consumer stopped with error", "error", err)
		os.Exit(1)
	}
}
