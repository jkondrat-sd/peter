package model

import (
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type DLQEntry struct {
	DLQID      string
	Event      Event
	ReasonCode string
	Reason     string
	RetryCount int64
	FailedAt   time.Time
}

func DLQEntryFromMessage(msg redis.XMessage) (DLQEntry, error) {
	event, err := EventFromMessage(msg)
	if err != nil {
		return DLQEntry{}, err
	}

	entry := DLQEntry{
		DLQID:    msg.ID,
		Event:    event,
		FailedAt: time.Now().UTC(),
	}

	if reasonCode, ok := msg.Values["reason_code"].(string); ok {
		entry.ReasonCode = reasonCode
	}
	if reason, ok := msg.Values["reason"].(string); ok {
		entry.Reason = reason
	}

	switch v := msg.Values["retry_count"].(type) {
	case string:
		if parsed, parseErr := strconv.ParseInt(v, 10, 64); parseErr == nil {
			entry.RetryCount = parsed
		}
	case int64:
		entry.RetryCount = v
	case int:
		entry.RetryCount = int64(v)
	}

	if rawFailedAt, ok := msg.Values["failed_at"].(string); ok && rawFailedAt != "" {
		if parsed, parseErr := time.Parse(time.RFC3339Nano, rawFailedAt); parseErr == nil {
			entry.FailedAt = parsed.UTC()
		}
	}

	return entry, nil
}

func (e Event) StreamValues() map[string]any {
	return map[string]any{
		"event_id":    e.EventID,
		"session_id":  e.SessionID,
		"payload":     e.Payload,
		"produced_at": e.ProducedAt.UTC().Format(time.RFC3339Nano),
	}
}

func MustEventFromDLQ(msg redis.XMessage) (Event, error) {
	entry, err := DLQEntryFromMessage(msg)
	if err != nil {
		return Event{}, fmt.Errorf("parse dlq entry %s: %w", msg.ID, err)
	}
	return entry.Event, nil
}
