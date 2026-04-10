package redisx

import (
	"context"
	"time"

	"redismq/internal/config"

	"github.com/redis/go-redis/v9"
)

// NewClient 提供抽象的单链或集群化模式兼容可横向扩展底层连接访问能力
func NewClient(cfg config.RedisConfig) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: cfg.Addr,
	})
}

// WaitForRedis 利用 Context 搭载支持非侵袭化停止退出控制手段，形成堵塞化高鲁棒性的连接探针体系策略
func WaitForRedis(ctx context.Context, rdb *redis.Client) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done(): 
			return ctx.Err()
		case <-ticker.C:
			if err := rdb.Ping(ctx).Err(); err == nil {
				return nil
			}
		}
	}
}
