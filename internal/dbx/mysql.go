package dbx

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type DBCluster struct {
	Master *sql.DB
	Slave  *sql.DB
}

func openSQL(dsn string) *sql.DB {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		slog.Error("MySQL Driver Initialization Failed", "dsn", dsn, "error", err)
		os.Exit(1)
	}
	db.SetMaxOpenConns(200)
	db.SetMaxIdleConns(50)
	db.SetConnMaxLifetime(30 * time.Minute)
	return db
}

// InitDBCluster 初始化配置针对写流 Master 与针对补偿读流 Slave 的独立连接池
func InitDBCluster(ctx context.Context) *DBCluster {
	masterDSN := os.Getenv("MYSQL_MASTER_DSN")
	if masterDSN == "" {
		masterDSN = "root:root_pass@tcp(localhost:3306)/audit_log"
	}
	slaveDSN := os.Getenv("MYSQL_SLAVE_DSN")
	if slaveDSN == "" {
		slaveDSN = "root:root_pass@tcp(localhost:3307)/audit_log"
	}

	cluster := &DBCluster{
		Master: openSQL(masterDSN),
		Slave:  openSQL(slaveDSN),
	}
	
	initializeSchema(ctx, cluster.Master)
	return cluster
}

func initializeSchema(ctx context.Context, masterDB *sql.DB) {
	query := `CREATE TABLE IF NOT EXISTS audit_logs (
		msg_id VARCHAR(100) PRIMARY KEY,
		session_id VARCHAR(100) NOT NULL,
		payload TEXT NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`
	
	// 阻塞重试建表保护隔离区
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := masterDB.Ping(); err == nil {
				_, err := masterDB.ExecContext(ctx, query)
				if err != nil {
					slog.Error("Failed to auto-create schema", "error", err)
				} else {
					slog.Info("Database schema established successfully on Master DB.")
					return
				}
			} else {
				slog.Warn("Waiting for MySQL Master to become fully booted via Bitnami Scripts...")
			}
		}
	}
}
