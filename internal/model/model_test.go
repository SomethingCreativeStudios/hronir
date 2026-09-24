package model

import (
	"testing"
	"time"
)

func TestRecordIDIsStableAndSourceScoped(t *testing.T) {
	first := RecordID("primary", "systems", "abc")
	if first != RecordID("primary", "systems", "abc") {
		t.Fatal("identity changed")
	}
	if first == RecordID("secondary", "systems", "abc") {
		t.Fatal("identity is not source-scoped")
	}
}

func TestRecordUsesDatastreamPhenomenonTime(t *testing.T) {
	record, err := NewRecord(Resource{Kind: "datastreams", ID: "stream", CanonicalURL: "https://source/datastreams/stream", PhenomenonTime: []any{"2026-01-01T00:00:00Z", ".."}}, "primary", "https://source", "digest", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if record["time"].(map[string]any)["interval"] == nil {
		t.Fatal("expected interval")
	}
}
