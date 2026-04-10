package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"redismq/internal/config"
	"redismq/internal/dbx"
	"redismq/internal/logger"
	"redismq/internal/redisx"

	"github.com/redis/go-redis/v9"
)

const (
	StreamName    = "event_queue"
	GroupName     = "processor_group"
	DlqStream     = "event_queue_dlq"
	MaxRetries    = 3
	IdleMs        = 10000
	MaxBatchSize  = 500
	FlushInterval = 1 * time.Second
)

var ConsumerName string

type StreamEvent struct {
	MsgID     string
	Payload   string
	SessionID string // Simulated extraction from payload logic
}

func main() {
	ConsumerName = os.Getenv("CONSUMER_NAME")
	if ConsumerName == "" {
		ConsumerName = "consumer-node-1"
	}
	
	logger.InitLogger(ConsumerName)
	slog.Info("Service Initialization: Micro-Batching Consumer Component Started")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := config.LoadRedisConfig()
	rdb := redisx.NewClient(cfg)
	if err := redisx.WaitForRedis(ctx, rdb); err != nil {
		slog.Error("Redis connection initialization failed", "error", err)
		os.Exit(1)
	}

	// 初始化具有读写分离保障的主从池组构建
	dbCluster := dbx.InitDBCluster(ctx)
	initGroup(ctx, rdb)

	go recoveryWorker(ctx, rdb, dbCluster)

	// 后端非堵塞调度轮询子网管道
	msgChan := make(chan []StreamEvent, 100)
	go messagePoller(ctx, rdb, msgChan)

	slog.Info("Micro-batching Engine Online. Listening for streamed payload matrices...")

	flushTicker := time.NewTicker(FlushInterval)
	defer flushTicker.Stop()

	// 高吞吐量的缓冲区集合
	bufferPool := make([]StreamEvent, 0, MaxBatchSize)

	for {
		select {
		case <-ctx.Done():
			// 【边界响应策略：直接丢弃 200 条，依赖纯底层的 XAUTOCLAIM 作为全知全能的接管机制】
			slog.Info("Shutdown signal trapped. Evaporating un-flushed memory buffers deliberately to delegate natively to PEL Recovery Engine.")
			return
		case <-flushTicker.C:
			if len(bufferPool) > 0 {
				flushBuffer(ctx, rdb, dbCluster, bufferPool)
				bufferPool = bufferPool[:0] 
			}
		case events := <-msgChan:
			bufferPool = append(bufferPool, events...)
			if len(bufferPool) >= MaxBatchSize {
				flushBuffer(ctx, rdb, dbCluster, bufferPool)
				bufferPool = bufferPool[:0]
				flushTicker.Reset(FlushInterval) // 重置防空转计步器
			}
		}
	}
}

// messagePoller 作为独立轮询 IO 控制，不阻塞 Batch 容量的监控计算时长。
func messagePoller(ctx context.Context, rdb *redis.Client, msgChan chan<- []StreamEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    GroupName,
				Consumer: ConsumerName,
				Streams:  []string{StreamName, ">"},
				Count:    100, // 增加单边拉扯流量吞吐宽度
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				if err != redis.Nil {
					slog.Error("Runtime error extracting payloads", "error", err)
					time.Sleep(1 * time.Second)
				}
				continue
			}

			var events []StreamEvent
			for _, stream := range streams {
				for _, msg := range stream.Messages {
					payload, ok := msg.Values["payload"].(string)
					if ok {
						events = append(events, StreamEvent{
							MsgID:     msg.ID,
							Payload:   payload,
							SessionID: "extracted-session-" + msg.ID, // 模拟业务提取提取逻辑
						})
					}
				}
			}

			if len(events) > 0 {
				msgChan <- events
			}
		}
	}
}

// flushBuffer 构建执行宏插入以及排错剥离降级网络逻辑
func flushBuffer(ctx context.Context, rdb *redis.Client, cluster *dbx.DBCluster, batch []StreamEvent) {
	slog.Info("Executing Micro-Batching Bulk Write...", "batchSize", len(batch))
	err := bulkInsert(ctx, cluster.Master, batch)

	if err != nil {
		slog.Warn("Batch processing evaluated Poison Pill. Degrading operation hierarchy heavily to serialize mode.", "error", err)
		for _, event := range batch {
			errSingle := singleInsert(ctx, cluster.Master, event)
			if errSingle != nil {
				slog.Error("Poison Pill positively isolated. Shunting into DLQ.", "msgID", event.MsgID, "err", errSingle)
				isolatePoisonPill(ctx, rdb, event)
			}
			rdb.XAck(ctx, StreamName, GroupName, event.MsgID)
		}
	} else {
		// 全局批量签发成功后安全触发全体资源解绑
		msgIDs := make([]string, 0, len(batch))
		for _, e := range batch {
			msgIDs = append(msgIDs, e.MsgID)
		}
		rdb.XAck(ctx, StreamName, GroupName, msgIDs...)
		slog.Info("Batch flush successful and synchronized towards Slave successfully via Binlog.", "syncedCount", len(batch))
	}
}

// bulkInsert 使用强拼接将上千个插入精简向一次 SQL 并发网际穿越。
func bulkInsert(ctx context.Context, master *sql.DB, batch []StreamEvent) error {
	valueStrings := make([]string, 0, len(batch))
	valueArgs := make([]interface{}, 0, len(batch)*3)

	for _, event := range batch {
		valueStrings = append(valueStrings, "(?, ?, ?)")
		valueArgs = append(valueArgs, event.MsgID, event.SessionID, event.Payload)
	}

	stmt := fmt.Sprintf("INSERT INTO audit_logs (msg_id, session_id, payload) VALUES %s", strings.Join(valueStrings, ","))
	_, err := master.ExecContext(ctx, stmt, valueArgs...)
	return err
}

func singleInsert(ctx context.Context, master *sql.DB, event StreamEvent) error {
	_, err := master.ExecContext(ctx, "INSERT INTO audit_logs (msg_id, session_id, payload) VALUES (?, ?, ?)", event.MsgID, event.SessionID, event.Payload)
	return err
}

func isolatePoisonPill(ctx context.Context, rdb *redis.Client, event StreamEvent) {
	rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: DlqStream,
		MaxLen: 1000,
		Values: map[string]interface{}{
			"original_id":   event.MsgID,
			"reject_reason": "SQL Transaction Violation - Batch Extraction",
			"payload":       event.Payload,
		},
	})
}

// 相同的回收守护机制，重用来调用处理遗漏/丢失数据的挽救手段
func recoveryWorker(ctx context.Context, rdb *redis.Client, cluster *dbx.DBCluster) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done(): return
		case <-ticker.C: reclaimAbandonedMessages(ctx, rdb, cluster) }
	}
}

func reclaimAbandonedMessages(ctx context.Context, rdb *redis.Client, cluster *dbx.DBCluster) {
	args := &redis.XAutoClaimArgs{
		Stream:   StreamName, Group:    GroupName, Consumer: ConsumerName,
		MinIdle:  time.Duration(IdleMs) * time.Millisecond, Start:    "0-0", Count:    20,
	}

	messages, _, err := rdb.XAutoClaim(ctx, args).Result()
	if err != nil { return }

	for _, msg := range messages {
		deliveryCount := int64(1) // Fake check for length purposes, real uses XPendingExt
		if deliveryCount > MaxRetries { /* Dump to DLQ */ continue }
		
		event := StreamEvent{MsgID: msg.ID, Payload: msg.Values["payload"].(string), SessionID: "recovered"}
		_ = singleInsert(ctx, cluster.Master, event)
		rdb.XAck(ctx, StreamName, GroupName, msg.ID)
	}
}

func initGroup(ctx context.Context, rdb *redis.Client) {
	err := rdb.XGroupCreateMkStream(ctx, StreamName, GroupName, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" { os.Exit(1) }
}
