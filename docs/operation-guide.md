# Redis MQ Audit System Operations & Runbook

This guide covers the deployment, configuration, operational management, troubleshooting, and telemetry visualization for the Redis-based Message Queue (MQ) Auditing System.

---

## 1. Quick Start & Local Deployment

The system is configured to run out-of-the-box using Docker Compose. This starts a localized Redis container, a MySQL replication setup (Master and Slave), along with Prometheus and Grafana for full monitoring metrics.

### Commands to Run the Cluster
From the root directory, execute the following commands in PowerShell:

```powershell
# Spin up all containers in detached mode
docker-compose up -d

# Verify all services are online and healthy
docker-compose ps
```

### Port Mappings
When deployed locally, the services expose the following default ports:

| Service | Internal Port | Host Port | Purpose |
|---------|---------------|-----------|---------|
| **Redis** | `6379` | `6379` | Message Broker & Streams |
| **MySQL Master** | `3306` | `3306` | Primary Audit Logs DB (Writes/Upserts) |
| **MySQL Slave** | `3306` | `3307` | Read Database (Idempotent auditing queries) |
| **Producer Metrics** | `2111` | `2111` | Telemetry scraper endpoint for the producer |
| **Consumer Metrics** | `2112` | `2112` | Telemetry scraper endpoint for the consumer |
| **Prometheus** | `9090` | `9090` | Time-series metrics collection database |
| **Grafana** | `3000` | `3000` | Telemetry visualization dashboard |

---

## 2. System Configurations Reference

Both the Producer and Consumer services are configured using environment variables loaded at startup.

### Producer Configurations (`internal/config/config.go`)
| Variable | Purpose | Default Value |
|----------|---------|---------------|
| `REDIS_HOST` | Host address of Redis | `localhost` |
| `REDIS_PORT` | Port of Redis | `6379` |
| `STREAM_NAME` | Name of the primary audit stream | `event_queue` |
| `PRODUCER_DEDUPE_TTL` | Enforced deduplication window | `5m` |
| `PRODUCER_WORKERS` | Count of concurrency routines | `50` |
| `PRODUCER_INTERVAL` | Delay between worker iterations | `10ms` |
| `METRICS_ADDR` | Prometheus HTTP endpoint address | `:2111` |

### Consumer Configurations (`internal/config/config.go`)
| Variable | Purpose | Default Value |
|----------|---------|---------------|
| `STREAM_NAME` | Target stream name | `event_queue` |
| `STREAM_GROUP` | Consumer group name | `processor_group` |
| `CONSUMER_NAME` | Identifier for this consumer instance | `consumer-node-1` |
| `DLQ_STREAM` | Dead letter stream name | `event_queue_dlq` |
| `BATCH_SIZE` | Max batch size inserted into MySQL | `500` |
| `FLUSH_INTERVAL` | Maximum delay before flush | `1s` |
| `READ_COUNT` | Batch block count read from stream | `100` |
| `READ_BLOCK` | XREADGROUP blocking timeout | `2s` |
| `RECLAIM_INTERVAL` | Frequency of PEL reclaiming scans | `5s` |
| `RECLAIM_MIN_IDLE` | Idle time threshold before claiming | `10s` |
| `RECLAIM_COUNT` | Max pending entries claimed per scan | `100` |
| `MAX_DELIVERIES` | Reclaim threshold before routing to DLQ | `5` |
| `DB_TIMEOUT` | MySQL statement execution timeout | `3s` |
| `METRICS_ADDR` | Prometheus HTTP endpoint address | `:2112` |
| `MYSQL_MASTER_DSN` | DSN connection string for Master | `root:root_pass@tcp(localhost:3306)/audit_log...` |
| `MYSQL_SLAVE_DSN` | DSN connection string for Slave | `root:root_pass@tcp(localhost:3307)/audit_log...` |

---

## 3. Operational Runbooks & Failure Simulation

### Runbook 1: Verification of Ingestion and Logs
To inspect if logs are correctly processing from Redis to MySQL:
1. Access the MySQL Master container:
   ```bash
   docker-compose exec mysql-master mysql -u root -proot_pass -D audit_log
   ```
2. Run a query to review the latest ingested events:
   ```sql
   SELECT event_id, stream_id, session_id, produced_at, created_at FROM audit_logs ORDER BY created_at DESC LIMIT 10;
   ```

### Runbook 2: Injecting Failures to Trigger DLQ Routing
To verify the system's resilience under DB outages:
1. Temporarily pause the MySQL Master DB container:
   ```powershell
   docker-compose pause mysql-master
   ```
2. The consumer batch writes will fail. Watch the consumer service log as it gracefully degrades to single-event insert retries:
   ```powershell
   docker-compose logs -f consumer
   ```
3. During this pause, messages will remain unacknowledged in the PEL. As the reclaim thread continually re-claims them, the delivery counter increments. Once it exceeds `MAX_DELIVERIES` (5), you will see the consumer isolate them into the Dead Letter Queue:
   ```
   "dlq routing occurred" consumer=consumer-node-1 reason_code=DB_CONN_TIMEOUT
   ```
4. Unpause the MySQL Master:
   ```powershell
   docker-compose unpause mysql-master
   ```
5. Regular queue pipelines will resume. The "poison pill" messages are now safely isolated in the DLQ and will not block normal events.

### Runbook 3: Executing DLQ Replay
Once DB connection is restored or the corrupted record is resolved, replay the isolated entries:
1. Execute the replay utility:
   ```powershell
   docker-compose run --rm dlq-replay
   ```
2. **Replay Config Parameters**:
   - `DLQ_REPLAY_COUNT` (Default: `100`): Defines how many dead events to process in this run.
   - `DLQ_REPLAY_KEEP_SOURCE` (Default: `false`): If set to `true`, the events will be re-injected but NOT deleted from the DLQ stream.
   To run with customized variables:
   ```powershell
   docker run --env DLQ_REPLAY_COUNT=50 --env DLQ_REPLAY_KEEP_SOURCE=true --network="host" redismq-dlq-replay
   ```

---

## 4. Telemetry, Prometheus & Grafana Monitoring

Telemetry metrics are exposed natively on ports `2111` (Producer) and `2112` (Consumer).

### Core Prometheus Metrics to Watch
- **`redismq_producer_published_total`** (Counter): Monitored production rate. A flatline indicates a producer stoppage.
- **`redismq_producer_duplicate_suppressed_total`** (Counter): Counts the duplicate transactions intercepted by client-side deduplication.
- **`redismq_consumer_events_handled_total{result="success/error"}`** (Counter): Main processing loop throughput rate.
- **`redismq_consumer_dlq_total`** (Counter): Tracks the rate of events isolated in the DLQ. Spikes in this metric indicate persistent database connection errors or severe data format issues.
- **`redismq_consumer_buffer_size`** (Gauge): Monitor to verify memory buffering limits are not exceeded during batch operations.
- **`redismq_consumer_db_write_seconds_bucket`** (Histogram): Tracks MySQL write response times. High latencies indicate a need for database tuning.

### Grafana Dashboards Integration
Grafana dashboards are automatically provisioned via the config mappings in [deployments/grafana](file:///f:/redis_mq/deployments/grafana).

1. Log into Grafana at `http://localhost:3000` (Default Credentials: `admin` / `admin`).
2. Go to **Dashboards** -> **Browse** -> Select **Redis MQ Audit Telemetry Dashboard**.
3. The dashboard features panels for:
   - **Ingress Rate**: Events generated per second.
   - **Database Insertion Latency**: Histograms tracing upsert durations.
   - **Consumer Lag**: Shows messages currently waiting in PEL.
   - **Dead Letter Isolation**: Track failures in real-time.
