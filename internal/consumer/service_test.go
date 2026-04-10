package consumer

import (
	"testing"

	"redismq/internal/model"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
)

func TestIsRetryableDBError(t *testing.T) {
	t.Parallel()

	if !isRetryableDBError(&mysqlDriver.MySQLError{Number: 1213, Message: "Deadlock found"}) {
		t.Fatal("expected deadlock to be retryable")
	}

	if isRetryableDBError(&mysqlDriver.MySQLError{Number: 1062, Message: "Duplicate entry"}) {
		t.Fatal("expected duplicate key to be non-retryable")
	}
}

func TestEventFromMessageRequiresMandatoryFields(t *testing.T) {
	t.Parallel()

	_, err := model.EventFromMessage(redis.XMessage{
		ID: "1-0",
		Values: map[string]any{
			"event_id": "evt-1",
			"payload":  `{"ok":true}`,
		},
	})
	if err == nil {
		t.Fatal("expected missing session_id to fail")
	}
}
