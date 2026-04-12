package main

import "testing"

func TestGetEnvBool(t *testing.T) {
	t.Setenv("DLQ_REPLAY_KEEP_SOURCE", "true")
	if !getEnvBool("DLQ_REPLAY_KEEP_SOURCE", false) {
		t.Fatal("expected env bool to parse true")
	}

	t.Setenv("DLQ_REPLAY_KEEP_SOURCE", "false")
	if getEnvBool("DLQ_REPLAY_KEEP_SOURCE", true) {
		t.Fatal("expected env bool to parse false")
	}
}
