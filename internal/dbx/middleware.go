package dbx

import (
	"context"
	"log/slog"
	"time"
)

// ExistsOnSlaveWithCompensation 用于 read-after-write 场景下的短暂补偿检查。
func ExistsOnSlaveWithCompensation(ctx context.Context, cluster *DBCluster, query string, args ...interface{}) (bool, error) {
	backoff := 50 * time.Millisecond

	for attempt := 1; attempt <= 3; attempt++ {
		var exists int
		err := cluster.Slave.QueryRowContext(ctx, query, args...).Scan(&exists)
		if err == nil {
			return true, nil
		}

		slog.Warn("slave record not visible yet; retrying", "attempt", attempt, "backoff", backoff.String(), "error", err)

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
		}
	}

	return false, nil
}
