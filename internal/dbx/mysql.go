package dbx

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"redismq/internal/config"
	"redismq/internal/model"

	_ "github.com/go-sql-driver/mysql"
)

type DBCluster struct {
	Master *sql.DB
	Slave  *sql.DB
}

func openSQL(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(200)
	db.SetMaxIdleConns(50)
	db.SetConnMaxLifetime(30 * time.Minute)
	return db, nil
}

func InitDBCluster(ctx context.Context, cfg config.DBConfig) (*DBCluster, error) {
	master, err := openSQL(cfg.MasterDSN)
	if err != nil {
		return nil, err
	}

	slave, err := openSQL(cfg.SlaveDSN)
	if err != nil {
		return nil, err
	}

	cluster := &DBCluster{
		Master: master,
		Slave:  slave,
	}

	if err := initializeSchema(ctx, cluster.Master); err != nil {
		return nil, err
	}
	return cluster, nil
}

func initializeSchema(ctx context.Context, masterDB *sql.DB) error {
	query := `CREATE TABLE IF NOT EXISTS audit_logs (
		event_id VARCHAR(100) PRIMARY KEY,
		stream_id VARCHAR(100) NOT NULL,
		session_id VARCHAR(100) NOT NULL,
		payload TEXT NOT NULL,
		produced_at TIMESTAMP NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		KEY idx_audit_logs_session_id (session_id),
		KEY idx_audit_logs_created_at (created_at)
	)`

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := masterDB.PingContext(ctx); err == nil {
				_, err := masterDB.ExecContext(ctx, query)
				if err != nil {
					slog.Error("Failed to auto-create schema", "error", err)
				} else {
					slog.Info("Database schema established successfully on Master DB.")
					return nil
				}
			} else {
				slog.Warn("Waiting for MySQL Master to become fully booted via Bitnami Scripts...")
			}
		}
	}
}

func UpsertAuditLogs(ctx context.Context, masterDB *sql.DB, events []model.Event) error {
	if len(events) == 0 {
		return nil
	}

	valueStrings := make([]string, 0, len(events))
	valueArgs := make([]interface{}, 0, len(events)*5)

	for _, event := range events {
		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?)")
		valueArgs = append(valueArgs, event.EventID, event.StreamID, event.SessionID, event.Payload, event.ProducedAt.UTC())
	}

	stmt := fmt.Sprintf(
		"INSERT INTO audit_logs (event_id, stream_id, session_id, payload, produced_at) VALUES %s "+
			"ON DUPLICATE KEY UPDATE stream_id = VALUES(stream_id), session_id = VALUES(session_id), payload = VALUES(payload), produced_at = VALUES(produced_at), updated_at = CURRENT_TIMESTAMP",
		strings.Join(valueStrings, ","),
	)
	_, err := masterDB.ExecContext(ctx, stmt, valueArgs...)
	return err
}
