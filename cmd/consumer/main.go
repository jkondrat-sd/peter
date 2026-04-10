package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"redismq/internal/config"
	"redismq/internal/logger"
	"redismq/internal/redisx"

	"github.com/redis/go-redis/v9"
)

const (
	StreamName = "event_queue"
	GroupName  = "processor_group"
	DlqStream  = "event_queue_dlq"
	MaxRetries = 3
	IdleMs     = 10000
)

var ConsumerName string

func main() {
	ConsumerName = os.Getenv("CONSUMER_NAME")
	if ConsumerName == "" {
		ConsumerName = "consumer-node-1"
	}
	
	logger.InitLogger(ConsumerName)
	slog.Info("Service Initialization: Consumer Component Started")

	cfg := config.LoadRedisConfig()
	rdb := redisx.NewClient(cfg)

	// 注册系统的 SIGTERM 和 SIGINT 退出信号绑定，用于协同业务模块的释放控制
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := redisx.WaitForRedis(ctx, rdb); err != nil {
		slog.Error("Redis connection initialization failed", "error", err)
		os.Exit(1)
	}

	initGroup(ctx, rdb)

	// 异步启动针对 PEL 机制的状态检查后台并发进程（故障转移/异常清理操作调度）
	go recoveryWorker(ctx, rdb)

	slog.Info("Consumer Ready. Listening for stream events...")

	// 包含堵塞边界策略与 Context 退坡检测的主消费循环
	for {
		select {
		case <-ctx.Done():
			slog.Info("Shutdown signal received. Flush pending requests and execute safe termination.")
			return
		default:
			processPendingBatch(ctx, rdb)
		}
	}
}

// initGroup 消费者群组配置模块，同时兼具状态游离兼容（静默处理已挂载产生的重合注册抛错反馈）的能力
func initGroup(ctx context.Context, rdb *redis.Client) {
	err := rdb.XGroupCreateMkStream(ctx, StreamName, GroupName, "0").Err()
	if err != nil {
		if err.Error() == "BUSYGROUP Consumer Group name already exists" {
			slog.Info("Consumer Group already exists. Attached safely.")
		} else {
			slog.Error("Group setup initialization failed", "error", err)
			os.Exit(1)
		}
	} else {
		slog.Info("Consumer Group explicitly created.")
	}
}

// processPendingBatch 针对 Stream 内队列元素的提取与向下的抽象应用派发控制
func processPendingBatch(ctx context.Context, rdb *redis.Client) {
	streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    GroupName,
		Consumer: ConsumerName,
		Streams:  []string{StreamName, ">"},
		Count:    1,
		Block:    2 * time.Second, // 防止无意义 CPU 周期自旋，同时避免过度持有长时链接导致的关闭盲阻塞
	}).Result()

	if err != nil {
		if err != redis.Nil { 
			slog.Error("Runtime error evaluating XREADGROUP", "error", err)
			time.Sleep(1 * time.Second)
		}
		return
	}

	for _, stream := range streams {
		for _, msg := range stream.Messages {
			payload, ok := msg.Values["payload"].(string)
			if !ok {
				slog.Warn("Malformed structured payload schema. Bypassing processing layer.")
				continue
			}

			// 一旦核心应用处理顺利，通过发送 XACK 反馈进行队列清空和系统资源下线操作
			if processMessage(payload) {
				rdb.XAck(ctx, StreamName, GroupName, msg.ID)
			}
		}
	}
}

// processMessage 作为提供内部抽象接口的隔离层，用于承载实现系统调用层的最终功能模块消费和逻辑完结部分
func processMessage(payload string) bool {
	slog.Info("Executing business operations with payload", "payload", payload)
	// 虚拟业务系统执行延迟以响应时序调停反馈校验
	time.Sleep(1 * time.Second)
	return true
}

// recoveryWorker 后端数据维护协程，接管因为宕机与错误而空置的数据消息以及监控引导死信机制执行
func recoveryWorker(ctx context.Context, rdb *redis.Client) {
	slog.Info("Recovery sentry initiated. Guarding stream consistency.")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Recovery worker terminating due to upwards cancellation cascade.")
			return 
		case <-ticker.C:
			reclaimAbandonedMessages(ctx, rdb)
		}
	}
}

// reclaimAbandonedMessages 执行 PEL 无活动滞留节点数据的抢占申明及死信判定的核心模块逻辑
func reclaimAbandonedMessages(ctx context.Context, rdb *redis.Client) {
	args := &redis.XAutoClaimArgs{
		Stream:   StreamName,
		Group:    GroupName,
		Consumer: ConsumerName,
		MinIdle:  time.Duration(IdleMs) * time.Millisecond,
		Start:    "0-0",
		Count:    10, // 控制单次调度认领条目，防止遭遇性能雪崩或接管惊群效应并发
	}

	messages, _, err := rdb.XAutoClaim(ctx, args).Result()
	if err != nil {
		slog.Error("XAUTOCLAIM native execution failure", "error", err)
		return
	}

	for _, msg := range messages {
		msgID := msg.ID
		payload := msg.Values["payload"].(string)

		pendingList, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: StreamName,
			Group:  GroupName,
			Start:  msgID,
			End:    msgID,
			Count:  1,
		}).Result()

		if err != nil || len(pendingList) == 0 {
			continue
		}

		deliveryCount := pendingList[0].RetryCount
		slog.Info("Processing transferred unacknowledged message", "msgID", msgID, "deliveryCount", deliveryCount)

		// 执行流判定，校验是否匹配放逐孤道机制的标准（超过警戒 Retry 阈值条件等特征要求）
		if deliveryCount > MaxRetries {
			slog.Warn("Message exceeded MaxRetries thresholds. Transiting to Dead Letter Queue securely.", "msgID", msgID)
			rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: DlqStream,
				MaxLen: 1000, 
				Approx: true,
				Values: map[string]interface{}{
					"original_id":   msgID,
					"reject_reason": "Max retries exceeded architectural constraints",
					"payload":       payload,
				},
			})
			rdb.XAck(ctx, StreamName, GroupName, msgID) 
			continue
		}

		if processMessage(payload) {
			rdb.XAck(ctx, StreamName, GroupName, msgID)
			slog.Info("Unacknowledged message handled and state cleared", "msgID", msgID)
		}
	}
}
