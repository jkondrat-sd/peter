package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	ProducerPublishedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "redismq_producer_published_total",
		Help: "Total number of stream events published by the producer.",
	})
	ProducerPublishFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "redismq_producer_publish_failed_total",
		Help: "Total number of producer publish failures.",
	})
	ProducerDuplicateSuppressedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "redismq_producer_duplicate_suppressed_total",
		Help: "Total number of duplicate events suppressed by producer-side dedupe.",
	})

	ConsumerEventsHandledTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "redismq_consumer_events_handled_total",
		Help: "Total number of consumer events handled by result.",
	}, []string{"consumer", "result"})
	ConsumerBatchFlushTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "redismq_consumer_batch_flush_total",
		Help: "Total number of batch flush attempts by result.",
	}, []string{"consumer", "result"})
	ConsumerDLQTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "redismq_consumer_dlq_total",
		Help: "Total number of events routed to DLQ.",
	}, []string{"consumer", "reason_code"})
	ConsumerReclaimTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "redismq_consumer_reclaim_total",
		Help: "Total number of pending messages reclaimed.",
	}, []string{"consumer", "result"})
	ConsumerPollErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "redismq_consumer_poll_errors_total",
		Help: "Total number of consumer poll errors.",
	}, []string{"consumer"})
	ConsumerBufferSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "redismq_consumer_buffer_size",
		Help: "Current in-memory buffer size.",
	}, []string{"consumer"})
	ConsumerPendingClaimed = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "redismq_consumer_pending_claimed",
		Help: "Number of pending messages claimed in the last reclaim cycle.",
	}, []string{"consumer"})
	ConsumerBatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "redismq_consumer_batch_size",
		Help:    "Observed consumer batch sizes.",
		Buckets: []float64{1, 10, 50, 100, 250, 500, 1000},
	}, []string{"consumer"})
	ConsumerDBWriteSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "redismq_consumer_db_write_seconds",
		Help:    "Latency of consumer DB writes.",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	}, []string{"consumer", "mode", "result"})
)

func StartServer(ctx context.Context, addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			slog.Error("metrics server shutdown failed", "addr", addr, "error", err)
		}
	}()

	go func() {
		slog.Info("metrics server listening", "addr", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server stopped", "addr", addr, "error", err)
		}
	}()
}
