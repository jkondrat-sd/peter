package config

import (
	"os"
	"strconv"
	"time"
)

type RedisConfig struct {
	Addr string
}

type DBConfig struct {
	MasterDSN string
	SlaveDSN  string
}

type ProducerConfig struct {
	StreamName  string
	DedupeTTL   time.Duration
	Workers     int
	Interval    time.Duration
	MetricsAddr string
	Redis       RedisConfig
}

type ConsumerConfig struct {
	StreamName           string
	GroupName            string
	ConsumerName         string
	DLQStream            string
	MaxBatchSize         int
	FlushInterval        time.Duration
	ReadCount            int64
	ReadBlock            time.Duration
	ReclaimInterval      time.Duration
	MinIdle              time.Duration
	ReclaimCount         int64
	MaxDeliveries        int64
	DBTimeout            time.Duration
	ShutdownFlushTimeout time.Duration
	MetricsAddr          string
	Redis                RedisConfig
	DB                   DBConfig
}

func LoadRedisConfig() RedisConfig {
	host := getEnv("REDIS_HOST", "localhost")
	port := getEnv("REDIS_PORT", "6379")
	return RedisConfig{
		Addr: host + ":" + port,
	}
}

func LoadDBConfig() DBConfig {
	return DBConfig{
		MasterDSN: getEnv("MYSQL_MASTER_DSN", "root:root_pass@tcp(localhost:3306)/audit_log?parseTime=true"),
		SlaveDSN:  getEnv("MYSQL_SLAVE_DSN", "root:root_pass@tcp(localhost:3307)/audit_log?parseTime=true"),
	}
}

func LoadProducerConfig() ProducerConfig {
	return ProducerConfig{
		StreamName:  getEnv("STREAM_NAME", "event_queue"),
		DedupeTTL:   getEnvDuration("PRODUCER_DEDUPE_TTL", 5*time.Minute),
		Workers:     getEnvInt("PRODUCER_WORKERS", 20),
		Interval:    getEnvDuration("PRODUCER_INTERVAL", 10*time.Millisecond),
		MetricsAddr: getEnv("METRICS_ADDR", ":2111"),
		Redis:       LoadRedisConfig(),
	}
}

func LoadConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		StreamName:           getEnv("STREAM_NAME", "event_queue"),
		GroupName:            getEnv("STREAM_GROUP", "processor_group"),
		ConsumerName:         getEnv("CONSUMER_NAME", "consumer-node-1"),
		DLQStream:            getEnv("DLQ_STREAM", "event_queue_dlq"),
		MaxBatchSize:         getEnvInt("BATCH_SIZE", 500),
		FlushInterval:        getEnvDuration("FLUSH_INTERVAL", time.Second),
		ReadCount:            int64(getEnvInt("READ_COUNT", 100)),
		ReadBlock:            getEnvDuration("READ_BLOCK", 2*time.Second),
		ReclaimInterval:      getEnvDuration("RECLAIM_INTERVAL", 5*time.Second),
		MinIdle:              getEnvDuration("RECLAIM_MIN_IDLE", 10*time.Second),
		ReclaimCount:         int64(getEnvInt("RECLAIM_COUNT", 100)),
		MaxDeliveries:        int64(getEnvInt("MAX_DELIVERIES", 5)),
		DBTimeout:            getEnvDuration("DB_TIMEOUT", 3*time.Second),
		ShutdownFlushTimeout: getEnvDuration("SHUTDOWN_FLUSH_TIMEOUT", 10*time.Second),
		MetricsAddr:          getEnv("METRICS_ADDR", ":2112"),
		Redis:                LoadRedisConfig(),
		DB:                   LoadDBConfig(),
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
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

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}

	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
