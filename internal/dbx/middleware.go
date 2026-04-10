package dbx

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"
)

// QueryWithCompensation 执行带有延迟补偿机制的查询代理
// 在发生读写分离 (RW Split) 的场景下，解决由于 Master 数据同步至 Slave 造成的百毫秒非同步幻读窗口。
func QueryWithCompensation(ctx context.Context, cluster *DBCluster, query string, args ...interface{}) (*sql.Rows, error) {
	var rows *sql.Rows
	var err error

	maxRetries := 3
	backoff := 50 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		rows, err = cluster.Slave.QueryContext(ctx, query, args...)
		
		// 校验查询状态，若命中有效数据强制成功释放
		if err == nil && rows.Next() {
			return rows, nil
		}
		if rows != nil {
			rows.Close()
		}

		slog.Warn("Slave record missing, compensating for replication lag window...", "attempt", i+1, "backoff", backoff.String())
		
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
			// 采用经典的自适应退避爬升（Exponential Backoff）延长扫描侦测间距
			backoff *= 2
		}
	}
	
	return nil, errors.New("record inherently missing on slave node after compensation threshold reached")
}
