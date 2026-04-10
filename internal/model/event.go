package model

import (
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Event struct {
	StreamID   string
	EventID    string
	SessionID  string
	Payload    string
	ProducedAt time.Time
}

func EventFromMessage(msg redis.XMessage) (Event, error) {
	eventID, ok := msg.Values["event_id"].(string)
	if !ok || eventID == "" {
		return Event{}, fmt.Errorf("missing event_id for stream message %s", msg.ID)
	}

	sessionID, ok := msg.Values["session_id"].(string)
	if !ok || sessionID == "" {
		return Event{}, fmt.Errorf("missing session_id for stream message %s", msg.ID)
	}

	payload, ok := msg.Values["payload"].(string)
	if !ok || payload == "" {
		return Event{}, fmt.Errorf("missing payload for stream message %s", msg.ID)
	}

	event := Event{
		StreamID:  msg.ID,
		EventID:   eventID,
		SessionID: sessionID,
		Payload:   payload,
	}

	if rawProducedAt, ok := msg.Values["produced_at"].(string); ok && rawProducedAt != "" {
		producedAt, err := time.Parse(time.RFC3339Nano, rawProducedAt)
		if err == nil {
			event.ProducedAt = producedAt.UTC()
		}
	}

	if event.ProducedAt.IsZero() {
		event.ProducedAt = time.Now().UTC()
	}

	return event, nil
}
