package state

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestCandidatesRequireTwoCompleteMisses(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	run, err := store.BeginRun(ctx, "source", "systems")
	if err != nil {
		t.Fatal(err)
	}
	err = store.Save(ctx, Resource{SourceID: "source", Kind: "systems", ResourceID: "one", CanonicalURL: "https://example/systems/one", RecordID: "record", NormalizedJSON: []byte(`{}`), NormalizedHash: "one", ProfileDigest: "profile", RecordHash: "record", LastSeenRun: run})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.BeginRun(ctx, "source", "systems")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CandidatesForDeletion(ctx, "source", "systems", first, 2); err != nil {
		t.Fatal(err)
	}
	second, err := store.BeginRun(ctx, "source", "systems")
	if err != nil {
		t.Fatal(err)
	}
	items, err := store.CandidatesForDeletion(ctx, "source", "systems", second, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("got %d candidates", len(items))
	}
}

func TestFailedEventsCanBeRetriedButCompletedEventsAreDeduplicated(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	event := Event{SourceID: "source", EventSource: "https://source", EventID: "one", Subject: "https://source/systems/one", Type: "org.ogc.api.consys.system.update", Payload: []byte(`{}`)}
	queued, err := store.EnqueueEvent(ctx, event)
	if err != nil || !queued {
		t.Fatalf("first queue: %v %v", queued, err)
	}
	if err := store.CompleteEvent(ctx, event, errors.New("temporary")); err != nil {
		t.Fatal(err)
	}
	queued, err = store.EnqueueEvent(ctx, event)
	if err != nil || !queued {
		t.Fatalf("failed event did not requeue: %v %v", queued, err)
	}
	if err := store.CompleteEvent(ctx, event, nil); err != nil {
		t.Fatal(err)
	}
	queued, err = store.EnqueueEvent(ctx, event)
	if err != nil || queued {
		t.Fatalf("completed event was not deduplicated: %v %v", queued, err)
	}
}
