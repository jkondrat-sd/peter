package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"redismq/internal/config"
	"redismq/internal/dbx"
	"redismq/internal/metrics"
	"redismq/internal/model"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
)

type Service struct {
	cfg config.ConsumerConfig
	rdb *redis.Client
	db  *dbx.DBCluster
}

func NewService(cfg config.ConsumerConfig, rdb *redis.Client, db *dbx.DBCluster) *Service {
	return &Service{cfg: cfg, rdb: rdb, db: db}
}

func (s *Service) Run(ctx context.Context) error {
	if err := s.initGroup(ctx); err != nil {
		return err
	}

	msgChan := make(chan []model.Event, 32)
	errChan := make(chan error, 1)

	go s.poll(ctx, msgChan, errChan)
	go s.runReclaimer(ctx)

	flushTicker := time.NewTicker(s.cfg.FlushInterval)
	defer flushTicker.Stop()

	buffer := make([]model.Event, 0, s.cfg.MaxBatchSize)

	for {
		select {
		case <-ctx.Done():
			return s.flushOnShutdown(buffer)
		case err := <-errChan:
			return err
		case <-flushTicker.C:
			buffer = s.flushBuffer(ctx, buffer)
			metrics.ConsumerBufferSize.WithLabelValues(s.cfg.ConsumerName).Set(float64(len(buffer)))
		case events := <-msgChan:
			buffer = append(buffer, events...)
			metrics.ConsumerBufferSize.WithLabelValues(s.cfg.ConsumerName).Set(float64(len(buffer)))
			for len(buffer) >= s.cfg.MaxBatchSize {
				buffer = s.flushBuffer(ctx, buffer)
				metrics.ConsumerBufferSize.WithLabelValues(s.cfg.ConsumerName).Set(float64(len(buffer)))
			}
		}
	}
}

func (s *Service) poll(ctx context.Context, msgChan chan<- []model.Event, errChan chan<- error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		streams, err := s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    s.cfg.GroupName,
			Consumer: s.cfg.ConsumerName,
			Streams:  []string{s.cfg.StreamName, ">"},
			Count:    s.cfg.ReadCount,
			Block:    s.cfg.ReadBlock,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) || errors.Is(err, context.Canceled) {
				continue
			}
			metrics.ConsumerPollErrorsTotal.WithLabelValues(s.cfg.ConsumerName).Inc()
			slog.Error("stream read failed", "error", err)
			time.Sleep(time.Second)
			continue
		}

		events := make([]model.Event, 0, s.cfg.MaxBatchSize)
		for _, stream := range streams {
			for _, msg := range stream.Messages {
				event, parseErr := model.EventFromMessage(msg)
				if parseErr != nil {
					metrics.ConsumerEventsHandledTotal.WithLabelValues(s.cfg.ConsumerName, "invalid_payload").Inc()
					slog.Error("invalid stream message, routing to dlq", "stream_id", msg.ID, "error", parseErr)
					if dlqErr := s.moveRawToDLQ(ctx, msg, "invalid_stream_payload", parseErr.Error(), 0); dlqErr != nil {
						select {
						case errChan <- dlqErr:
						default:
						}
						return
					}
					continue
				}
				events = append(events, event)
			}
		}

		if len(events) == 0 {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case msgChan <- events:
		}
	}
}

func (s *Service) flushOnShutdown(buffer []model.Event) error {
	if len(buffer) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownFlushTimeout)
	defer cancel()

	remaining := s.flushBuffer(ctx, buffer)
	if len(remaining) > 0 {
		return fmt.Errorf("shutdown flush left %d pending events in the stream", len(remaining))
	}
	return nil
}

func (s *Service) flushBuffer(ctx context.Context, buffer []model.Event) []model.Event {
	if len(buffer) == 0 {
		return buffer
	}

	batchSize := min(len(buffer), s.cfg.MaxBatchSize)
	batch := append([]model.Event(nil), buffer[:batchSize]...)
	metrics.ConsumerBatchSize.WithLabelValues(s.cfg.ConsumerName).Observe(float64(len(batch)))

	err := s.persistBatch(ctx, batch)
	if err == nil {
		metrics.ConsumerBatchFlushTotal.WithLabelValues(s.cfg.ConsumerName, "success").Inc()
		metrics.ConsumerEventsHandledTotal.WithLabelValues(s.cfg.ConsumerName, "persisted").Add(float64(len(batch)))
		return buffer[batchSize:]
	}

	if isRetryableDBError(err) {
		metrics.ConsumerBatchFlushTotal.WithLabelValues(s.cfg.ConsumerName, "retryable_error").Inc()
		slog.Warn("batch persistence failed with retryable error; leaving messages pending", "batch_size", len(batch), "error", err)
		return buffer
	}

	metrics.ConsumerBatchFlushTotal.WithLabelValues(s.cfg.ConsumerName, "degraded").Inc()
	slog.Warn("batch persistence failed with non-retryable error; degrading to single message handling", "batch_size", len(batch), "error", err)
	for _, event := range batch {
		s.handleSingle(ctx, event, 0)
	}
	return buffer[batchSize:]
}

func (s *Service) persistBatch(ctx context.Context, events []model.Event) error {
	dbCtx, cancel := context.WithTimeout(ctx, s.cfg.DBTimeout)
	defer cancel()

	start := time.Now()
	if err := dbx.UpsertAuditLogs(dbCtx, s.db.Master, events); err != nil {
		metrics.ConsumerDBWriteSeconds.WithLabelValues(s.cfg.ConsumerName, "batch", "error").Observe(time.Since(start).Seconds())
		return err
	}
	metrics.ConsumerDBWriteSeconds.WithLabelValues(s.cfg.ConsumerName, "batch", "success").Observe(time.Since(start).Seconds())

	return s.ack(dbCtx, events...)
}

func (s *Service) handleSingle(ctx context.Context, event model.Event, retryCount int64) {
	dbCtx, cancel := context.WithTimeout(ctx, s.cfg.DBTimeout)
	defer cancel()

	start := time.Now()
	err := dbx.UpsertAuditLogs(dbCtx, s.db.Master, []model.Event{event})
	switch {
	case err == nil:
		metrics.ConsumerDBWriteSeconds.WithLabelValues(s.cfg.ConsumerName, "single", "success").Observe(time.Since(start).Seconds())
		metrics.ConsumerEventsHandledTotal.WithLabelValues(s.cfg.ConsumerName, "persisted").Inc()
		if ackErr := s.ack(dbCtx, event); ackErr != nil {
			slog.Error("ack failed after single event persistence", "stream_id", event.StreamID, "error", ackErr)
		}
	case isRetryableDBError(err):
		metrics.ConsumerDBWriteSeconds.WithLabelValues(s.cfg.ConsumerName, "single", "error").Observe(time.Since(start).Seconds())
		metrics.ConsumerEventsHandledTotal.WithLabelValues(s.cfg.ConsumerName, "retry_pending").Inc()
		slog.Warn("retryable single event persistence failure; keeping event pending", "stream_id", event.StreamID, "error", err)
	default:
		metrics.ConsumerDBWriteSeconds.WithLabelValues(s.cfg.ConsumerName, "single", "error").Observe(time.Since(start).Seconds())
		metrics.ConsumerEventsHandledTotal.WithLabelValues(s.cfg.ConsumerName, "dlq").Inc()
		slog.Error("non-retryable single event persistence failure; routing to dlq", "stream_id", event.StreamID, "error", err)
		if dlqErr := s.moveEventToDLQ(ctx, event, "db_persist_failure", err.Error(), retryCount); dlqErr != nil {
			slog.Error("failed to route event to dlq", "stream_id", event.StreamID, "error", dlqErr)
		}
	}
}

func (s *Service) runReclaimer(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.ReclaimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.reclaimPending(ctx)
		}
	}
}

func (s *Service) reclaimPending(ctx context.Context) {
	pendingEntries, err := s.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: s.cfg.StreamName,
		Group:  s.cfg.GroupName,
		Idle:   s.cfg.MinIdle,
		Start:  "-",
		End:    "+",
		Count:  s.cfg.ReclaimCount,
	}).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) && !errors.Is(err, context.Canceled) {
			slog.Error("pending scan failed", "error", err)
		}
		return
	}
	if len(pendingEntries) == 0 {
		metrics.ConsumerPendingClaimed.WithLabelValues(s.cfg.ConsumerName).Set(0)
		return
	}

	retryByID := make(map[string]int64, len(pendingEntries))
	ids := make([]string, 0, len(pendingEntries))
	for _, entry := range pendingEntries {
		ids = append(ids, entry.ID)
		retryByID[entry.ID] = entry.RetryCount
	}

	messages, err := s.rdb.XClaim(ctx, &redis.XClaimArgs{
		Stream:   s.cfg.StreamName,
		Group:    s.cfg.GroupName,
		Consumer: s.cfg.ConsumerName,
		MinIdle:  s.cfg.MinIdle,
		Messages: ids,
	}).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) && !errors.Is(err, context.Canceled) {
			slog.Error("claim pending messages failed", "error", err)
		}
		return
	}
	metrics.ConsumerPendingClaimed.WithLabelValues(s.cfg.ConsumerName).Set(float64(len(messages)))

	for _, msg := range messages {
		retryCount := retryByID[msg.ID]
		event, parseErr := model.EventFromMessage(msg)
		if parseErr != nil {
			metrics.ConsumerReclaimTotal.WithLabelValues(s.cfg.ConsumerName, "invalid_payload").Inc()
			slog.Error("claimed message is invalid; moving to dlq", "stream_id", msg.ID, "error", parseErr)
			if err := s.moveRawToDLQ(ctx, msg, "invalid_stream_payload", parseErr.Error(), retryCount); err != nil {
				slog.Error("failed to move invalid claimed message to dlq", "stream_id", msg.ID, "error", err)
			}
			continue
		}

		if retryCount > s.cfg.MaxDeliveries {
			metrics.ConsumerReclaimTotal.WithLabelValues(s.cfg.ConsumerName, "max_deliveries_exceeded").Inc()
			slog.Error("max delivery count exceeded; moving event to dlq", "stream_id", event.StreamID, "retry_count", retryCount)
			if err := s.moveEventToDLQ(ctx, event, "max_deliveries_exceeded", "delivery count exceeded", retryCount); err != nil {
				slog.Error("failed to move over-retried event to dlq", "stream_id", event.StreamID, "error", err)
			}
			continue
		}

		metrics.ConsumerReclaimTotal.WithLabelValues(s.cfg.ConsumerName, "reprocessed").Inc()
		s.handleSingle(ctx, event, retryCount)
	}
}

func (s *Service) moveEventToDLQ(ctx context.Context, event model.Event, code, reason string, retryCount int64) error {
	if _, err := s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: s.cfg.DLQStream,
		Values: map[string]any{
			"stream_id":     event.StreamID,
			"event_id":      event.EventID,
			"session_id":    event.SessionID,
			"payload":       event.Payload,
			"reason_code":   code,
			"reason":        reason,
			"retry_count":   retryCount,
			"failed_at":     time.Now().UTC().Format(time.RFC3339Nano),
			"consumer_name": s.cfg.ConsumerName,
		},
		MaxLen: 10_000,
	}).Result(); err != nil {
		return err
	}
	metrics.ConsumerDLQTotal.WithLabelValues(s.cfg.ConsumerName, code).Inc()

	return s.ack(ctx, event)
}

func (s *Service) moveRawToDLQ(ctx context.Context, msg redis.XMessage, code, reason string, retryCount int64) error {
	payload := rawValues(msg.Values)
	if _, err := s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: s.cfg.DLQStream,
		Values: map[string]any{
			"stream_id":     msg.ID,
			"payload":       payload,
			"reason_code":   code,
			"reason":        reason,
			"retry_count":   retryCount,
			"failed_at":     time.Now().UTC().Format(time.RFC3339Nano),
			"consumer_name": s.cfg.ConsumerName,
		},
		MaxLen: 10_000,
	}).Result(); err != nil {
		return err
	}
	metrics.ConsumerDLQTotal.WithLabelValues(s.cfg.ConsumerName, code).Inc()

	return s.rdb.XAck(ctx, s.cfg.StreamName, s.cfg.GroupName, msg.ID).Err()
}

func (s *Service) ack(ctx context.Context, events ...model.Event) error {
	if len(events) == 0 {
		return nil
	}

	ids := make([]string, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.StreamID)
	}
	return s.rdb.XAck(ctx, s.cfg.StreamName, s.cfg.GroupName, ids...).Err()
}

func (s *Service) initGroup(ctx context.Context) error {
	err := s.rdb.XGroupCreateMkStream(ctx, s.cfg.StreamName, s.cfg.GroupName, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func rawValues(values map[string]any) string {
	parts := make([]string, 0, len(values))
	for key, value := range values {
		parts = append(parts, fmt.Sprintf("%s=%v", key, value))
	}
	return strings.Join(parts, ",")
}

func isRetryableDBError(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}

	var mysqlErr *mysqlDriver.MySQLError
	if errors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1040, 1042, 1047, 1158, 1159, 1160, 1161, 1205, 1213, 2002, 2003, 2006, 2013:
			return true
		}
	}

	retryablePhrases := []string{
		"connection refused",
		"broken pipe",
		"invalid connection",
		"connection reset",
		"i/o timeout",
		"too many connections",
	}
	lower := strings.ToLower(err.Error())
	for _, phrase := range retryablePhrases {
		if strings.Contains(lower, phrase) {
			return true
		}
	}

	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
