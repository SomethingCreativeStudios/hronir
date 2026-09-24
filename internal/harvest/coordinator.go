// Package harvest coordinates source crawling, mapping, durable state, and target writes.
package harvest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/SomethingCreativeStudios/hronir/internal/config"
	"github.com/SomethingCreativeStudios/hronir/internal/events"
	"github.com/SomethingCreativeStudios/hronir/internal/mapping"
	"github.com/SomethingCreativeStudios/hronir/internal/model"
	"github.com/SomethingCreativeStudios/hronir/internal/sink"
	"github.com/SomethingCreativeStudios/hronir/internal/source"
	"github.com/SomethingCreativeStudios/hronir/internal/state"
	"golang.org/x/sync/errgroup"
)

type Metrics interface {
	Crawl(string, string, bool)
	Materialized(string, string)
	Failed(string, string)
	Deleted(string, string)
	Event(string, string)
}
type nopMetrics struct{}

func (nopMetrics) Crawl(string, string, bool)  {}
func (nopMetrics) Materialized(string, string) {}
func (nopMetrics) Failed(string, string)       {}
func (nopMetrics) Deleted(string, string)      {}
func (nopMetrics) Event(string, string)        {}

type Coordinator struct {
	config  config.Config
	store   *state.Store
	mapper  *mapping.Engine
	sink    sink.Sink
	logger  *slog.Logger
	metrics Metrics
	locks   sync.Map
}

func New(cfg config.Config, store *state.Store, mapper *mapping.Engine, target sink.Sink, logger *slog.Logger, metrics Metrics) *Coordinator {
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		metrics = nopMetrics{}
	}
	return &Coordinator{config: cfg, store: store, mapper: mapper, sink: target, logger: logger, metrics: metrics}
}
func (c *Coordinator) CheckTarget(ctx context.Context) error { return c.sink.Check(ctx) }

func (c *Coordinator) Sync(ctx context.Context, sourceFilter, kindFilter string) error {
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(c.config.Runtime.Concurrency)
	for _, sourceConfig := range c.config.Sources {
		sourceConfig := sourceConfig
		if sourceFilter != "" && sourceConfig.ID != sourceFilter {
			continue
		}
		group.Go(func() error { return c.syncSource(groupCtx, sourceConfig, kindFilter) })
	}
	return group.Wait()
}
func (c *Coordinator) syncSource(ctx context.Context, sourceConfig config.SourceConfig, kindFilter string) error {
	unlock := c.lock(sourceConfig.ID)
	defer unlock()
	client, err := source.New(ctx, sourceConfig, c.config.Runtime.RequestTimeout)
	if err != nil {
		return err
	}
	if err := client.Discover(ctx); err != nil {
		c.logger.Warn("source discovery failed; standard paths will be used", "source", sourceConfig.ID, "error", err)
	}
	var sourceErrs []error
	for _, kind := range config.ResourceKinds {
		if kindFilter != "" && kind != kindFilter {
			continue
		}
		if sourceConfig.Resources[kind].Mode == "disabled" {
			continue
		}
		if err := c.crawlKind(ctx, client, sourceConfig, kind); err != nil {
			sourceErrs = append(sourceErrs, err)
		}
	}
	return errors.Join(sourceErrs...)
}
func (c *Coordinator) crawlKind(ctx context.Context, client *source.Client, sourceConfig config.SourceConfig, kind string) error {
	runID, err := c.store.BeginRun(ctx, sourceConfig.ID, kind)
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		_ = c.store.FinishRun(context.Background(), runID, complete)
		c.metrics.Crawl(sourceConfig.ID, kind, complete)
	}()
	result, err := client.CrawlKind(ctx, kind, func(resource model.Resource) error { return c.materialize(ctx, sourceConfig, resource, runID, true) })
	if err != nil {
		return err
	}
	complete = result.Complete
	if !complete {
		return fmt.Errorf("crawl %s did not complete", kind)
	}
	if result.Skipped {
		c.logger.Info("source does not expose optional resource kind; preserving existing records", "source", sourceConfig.ID, "kind", kind)
		return nil
	}
	candidates, err := c.store.CandidatesForDeletion(ctx, sourceConfig.ID, kind, runID, c.config.Runtime.DeleteAfterSuccessfulMisses)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		if err := c.sink.Delete(ctx, candidate.RecordID); err != nil {
			return fmt.Errorf("delete %s: %w", candidate.RecordID, err)
		}
		if err := c.store.MarkDeleted(ctx, candidate.SourceID, candidate.Kind, candidate.ResourceID); err != nil {
			return err
		}
		c.metrics.Deleted(candidate.SourceID, candidate.Kind)
		c.logger.Info("deleted stale record", "source", candidate.SourceID, "kind", candidate.Kind, "record", candidate.RecordID)
	}
	return nil
}
func (c *Coordinator) materialize(ctx context.Context, sourceConfig config.SourceConfig, resource model.Resource, runID int64, markSeen bool) error {
	normalized, err := model.CanonicalJSON(resource)
	if err != nil {
		return err
	}
	normalizedHash, err := model.Hash(resource)
	if err != nil {
		return err
	}
	previous, found, err := c.store.Get(ctx, sourceConfig.ID, resource.Kind, resource.ID)
	if err != nil {
		return err
	}
	lastSeen := previous.LastSeenRun
	if markSeen {
		lastSeen = runID
	}
	record, err := c.mapper.Map(resource, sourceConfig.ID, strings.TrimRight(sourceConfig.URL, "/"), time.Now())
	if err != nil {
		failure := state.Resource{SourceID: sourceConfig.ID, Kind: resource.Kind, ResourceID: resource.ID, CanonicalURL: resource.CanonicalURL, RecordID: model.RecordID(sourceConfig.ID, resource.Kind, resource.ID), NormalizedJSON: normalized, NormalizedHash: normalizedHash, ProfileDigest: c.mapper.ProfileDigest(), RecordHash: previous.RecordHash, LastSeenRun: lastSeen, ConsecutiveMisses: 0, Status: "failed", LastError: err.Error()}
		if found {
			failure.Deleted = previous.Deleted
		}
		_ = c.store.Save(ctx, failure)
		c.metrics.Failed(sourceConfig.ID, resource.Kind)
		return nil
	}
	recordHash, err := model.SemanticHash(record)
	if err != nil {
		return err
	}
	model.SetHashes(record, normalizedHash, recordHash)
	if found && previous.Status == "active" && !previous.Deleted && previous.NormalizedHash == normalizedHash && previous.RecordHash == recordHash {
		previous.LastSeenRun = lastSeen
		previous.ConsecutiveMisses = 0
		previous.CanonicalURL = resource.CanonicalURL
		return c.store.Save(ctx, previous)
	}
	if err := c.sink.Put(ctx, model.RecordID(sourceConfig.ID, resource.Kind, resource.ID), record); err != nil {
		failure := state.Resource{SourceID: sourceConfig.ID, Kind: resource.Kind, ResourceID: resource.ID, CanonicalURL: resource.CanonicalURL, RecordID: model.RecordID(sourceConfig.ID, resource.Kind, resource.ID), NormalizedJSON: normalized, NormalizedHash: normalizedHash, ProfileDigest: c.mapper.ProfileDigest(), RecordHash: previous.RecordHash, LastSeenRun: lastSeen, ConsecutiveMisses: 0, Status: "failed", LastError: err.Error()}
		if found {
			failure.Deleted = previous.Deleted
		}
		_ = c.store.Save(ctx, failure)
		c.metrics.Failed(sourceConfig.ID, resource.Kind)
		return nil
	}
	saved := state.Resource{SourceID: sourceConfig.ID, Kind: resource.Kind, ResourceID: resource.ID, CanonicalURL: resource.CanonicalURL, RecordID: model.RecordID(sourceConfig.ID, resource.Kind, resource.ID), NormalizedJSON: normalized, NormalizedHash: normalizedHash, ProfileDigest: c.mapper.ProfileDigest(), RecordHash: recordHash, LastSeenRun: lastSeen, ConsecutiveMisses: 0, Status: "active", Deleted: false}
	if err := c.store.Save(ctx, saved); err != nil {
		return err
	}
	c.metrics.Materialized(sourceConfig.ID, resource.Kind)
	return nil
}

// HandleCloudEvent records a durable event, then reconciles the current subject.
func (c *Coordinator) HandleCloudEvent(ctx context.Context, sourceID string, payload []byte) error {
	event, err := events.Parse(payload)
	if err != nil {
		return err
	}
	kind, ok := events.ResourceKind(event.Type)
	if !ok {
		return fmt.Errorf("ignore unsupported event type %q", event.Type)
	}
	sourceConfig, ok := c.source(sourceID)
	if !ok {
		return fmt.Errorf("unknown source %q", sourceID)
	}
	if sourceConfig.Resources[kind].Mode == "disabled" {
		return nil
	}
	if !eventOriginMatches(sourceConfig.URL, event.Source) {
		return errors.New("CloudEvent source has a different origin")
	}
	queued, err := c.store.EnqueueEvent(ctx, state.Event{SourceID: sourceID, EventSource: event.Source, EventID: event.ID, Subject: event.Subject, Type: event.Type, Payload: payload})
	if err != nil || !queued {
		return err
	}
	c.metrics.Event(sourceID, kind)
	unlock := c.lock(sourceID)
	defer unlock()
	client, err := source.New(ctx, sourceConfig, c.config.Runtime.RequestTimeout)
	if err == nil {
		var resource model.Resource
		var found bool
		resource, found, err = client.GetSubject(ctx, event.Subject, kind)
		if err == nil && found {
			err = c.materialize(ctx, sourceConfig, resource, 0, false)
		} else if err == nil && !found {
			var existing state.Resource
			var exists bool
			existing, exists, err = c.store.GetByURL(ctx, sourceID, resolveEventSubject(sourceConfig.URL, event.Subject))
			if err == nil && exists {
				err = c.sink.Delete(ctx, existing.RecordID)
				if err == nil {
					err = c.store.MarkDeleted(ctx, existing.SourceID, existing.Kind, existing.ResourceID)
				}
			}
		}
	}
	_ = c.store.CompleteEvent(context.Background(), state.Event{SourceID: sourceID, EventSource: event.Source, EventID: event.ID}, err)
	return err
}
func (c *Coordinator) source(id string) (config.SourceConfig, bool) {
	for _, source := range c.config.Sources {
		if source.ID == id {
			return source, true
		}
	}
	return config.SourceConfig{}, false
}
func (c *Coordinator) lock(id string) func() {
	value, _ := c.locks.LoadOrStore(id, &sync.Mutex{})
	mutex := value.(*sync.Mutex)
	mutex.Lock()
	return mutex.Unlock
}

func (c *Coordinator) ReconcileTarget(ctx context.Context) error {
	items, err := c.store.Active(ctx)
	if err != nil {
		return err
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(c.config.Runtime.Concurrency)
	for _, item := range items {
		item := item
		group.Go(func() error {
			sourceConfig, found := c.source(item.SourceID)
			if !found {
				return nil
			}
			var resource model.Resource
			if err := json.Unmarshal(item.NormalizedJSON, &resource); err != nil {
				return err
			}
			record, err := c.mapper.Map(resource, sourceConfig.ID, strings.TrimRight(sourceConfig.URL, "/"), time.Now())
			if err != nil {
				return err
			}
			recordHash, err := model.SemanticHash(record)
			if err != nil {
				return err
			}
			targetRecord, present, err := c.sink.Get(groupCtx, item.RecordID)
			if err != nil {
				return err
			}
			if present && targetHash(targetRecord) == recordHash {
				return nil
			}
			model.SetHashes(record, item.NormalizedHash, recordHash)
			if err := c.sink.Put(groupCtx, item.RecordID, record); err != nil {
				return err
			}
			item.RecordHash = recordHash
			item.ProfileDigest = c.mapper.ProfileDigest()
			return c.store.Save(groupCtx, item)
		})
	}
	return group.Wait()
}

// Prune deliberately removes materialized records only after an explicit CLI request.
func (c *Coordinator) Prune(ctx context.Context, sourceID, kind string) (int, error) {
	items, err := c.store.ActiveFor(ctx, sourceID, kind)
	if err != nil {
		return 0, err
	}
	for _, item := range items {
		if err := c.sink.Delete(ctx, item.RecordID); err != nil {
			return 0, err
		}
		if err := c.store.MarkDeleted(ctx, item.SourceID, item.Kind, item.ResourceID); err != nil {
			return 0, err
		}
		c.metrics.Deleted(item.SourceID, item.Kind)
	}
	return len(items), nil
}

// RetryFailures reuses cached normalized resources and never needs a source crawl.
func (c *Coordinator) RetryFailures(ctx context.Context) error {
	items, err := c.store.Failures(ctx)
	if err != nil {
		return err
	}
	for _, item := range items {
		sourceConfig, found := c.source(item.SourceID)
		if !found {
			continue
		}
		var resource model.Resource
		if err := json.Unmarshal(item.NormalizedJSON, &resource); err != nil {
			return err
		}
		if err := c.materialize(ctx, sourceConfig, resource, item.LastSeenRun, false); err != nil {
			return err
		}
	}
	return nil
}

func (c *Coordinator) RetryEvents(ctx context.Context) error {
	items, err := c.store.RetryableEvents(ctx)
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := c.HandleCloudEvent(ctx, item.SourceID, item.Payload); err != nil {
			c.logger.Warn("event retry failed", "source", item.SourceID, "event", item.EventID, "error", err)
		}
	}
	return nil
}
func targetHash(record model.Record) string {
	properties, _ := record["properties"].(map[string]any)
	hronir, _ := properties["hronir"].(map[string]any)
	value, _ := hronir["recordHash"].(string)
	return value
}
func eventOriginMatches(base, eventSource string) bool {
	eventURL, err := url.Parse(eventSource)
	if err != nil {
		return false
	}
	baseURL, err := url.Parse(base)
	if err != nil {
		return false
	}
	resolved := baseURL.ResolveReference(eventURL)
	return resolved.Scheme == baseURL.Scheme && resolved.Host == baseURL.Host
}
func resolveEventSubject(base, subject string) string {
	baseURL, _ := url.Parse(base)
	subjectURL, _ := url.Parse(subject)
	return baseURL.ResolveReference(subjectURL).String()
}

func (c *Coordinator) Run(ctx context.Context, onReady func()) error {
	if err := c.CheckTarget(ctx); err != nil {
		return err
	}
	if onReady != nil {
		onReady()
	}
	if err := c.Sync(ctx, "", ""); err != nil {
		c.logger.Error("initial synchronization failed", "error", err)
	}
	if err := c.RetryEvents(ctx); err != nil {
		c.logger.Error("initial event retry failed", "error", err)
	}
	poll := time.NewTicker(jitter(c.config.Runtime.PollInterval))
	defer poll.Stop()
	reconcile := time.NewTicker(c.config.Runtime.TargetReconciliationInterval)
	defer reconcile.Stop()
	eventRetry := time.NewTicker(time.Minute)
	defer eventRetry.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
			if err := c.Sync(ctx, "", ""); err != nil {
				c.logger.Error("synchronization failed", "error", err)
			}
			poll.Reset(jitter(c.config.Runtime.PollInterval))
		case <-reconcile.C:
			if err := c.ReconcileTarget(ctx); err != nil {
				c.logger.Error("target reconciliation failed", "error", err)
			}
		case <-eventRetry.C:
			if err := c.RetryEvents(ctx); err != nil {
				c.logger.Error("event retry failed", "error", err)
			}
		}
	}
}
func jitter(duration time.Duration) time.Duration {
	return duration - time.Duration(rand.Int64N(int64(duration)/10+1))
}
