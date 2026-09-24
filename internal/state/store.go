// Package state provides Hronir's durable, single-writer SQLite state.
package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

type Store struct {
	db   *sql.DB
	lock *os.File
}

type Resource struct {
	SourceID          string
	Kind              string
	ResourceID        string
	CanonicalURL      string
	RecordID          string
	NormalizedJSON    []byte
	NormalizedHash    string
	ProfileDigest     string
	RecordHash        string
	LastSeenRun       int64
	ConsecutiveMisses int
	Status            string
	LastError         string
	Deleted           bool
}

type Event struct {
	SourceID    string
	EventSource string
	EventID     string
	Subject     string
	Type        string
	Payload     []byte
	Status      string
	LastError   string
}

func Open(path string) (*Store, error) {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("acquire state lock: another Hronir process may own %s: %w", path, err)
	}
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)")
	if err != nil {
		lock.Close()
		return nil, err
	}
	store := &Store{db: db, lock: lock}
	if err := store.migrate(context.Background()); err != nil {
		store.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	var result error
	if s.db != nil {
		result = s.db.Close()
	}
	if s.lock != nil {
		_ = unix.Flock(int(s.lock.Fd()), unix.LOCK_UN)
		_ = s.lock.Close()
	}
	return result
}
func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS runs (
 id INTEGER PRIMARY KEY AUTOINCREMENT, source_id TEXT NOT NULL, kind TEXT NOT NULL,
 started_at TEXT NOT NULL, finished_at TEXT, complete INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS resources (
 source_id TEXT NOT NULL, kind TEXT NOT NULL, resource_id TEXT NOT NULL,
 canonical_url TEXT NOT NULL, record_id TEXT NOT NULL, normalized_json BLOB NOT NULL,
 normalized_hash TEXT NOT NULL, profile_digest TEXT NOT NULL, record_hash TEXT NOT NULL DEFAULT '',
 last_seen_run INTEGER NOT NULL DEFAULT 0, consecutive_misses INTEGER NOT NULL DEFAULT 0,
 status TEXT NOT NULL, last_error TEXT NOT NULL DEFAULT '', deleted INTEGER NOT NULL DEFAULT 0,
 updated_at TEXT NOT NULL,
 PRIMARY KEY(source_id, kind, resource_id)
);
CREATE INDEX IF NOT EXISTS resources_active ON resources(source_id, kind, deleted, last_seen_run);
CREATE UNIQUE INDEX IF NOT EXISTS resources_canonical_url ON resources(source_id, canonical_url);
CREATE TABLE IF NOT EXISTS events (
 source_id TEXT NOT NULL, event_source TEXT NOT NULL, event_id TEXT NOT NULL,
 subject TEXT NOT NULL, event_type TEXT NOT NULL, payload BLOB NOT NULL,
 status TEXT NOT NULL, last_error TEXT NOT NULL DEFAULT '', updated_at TEXT NOT NULL,
 PRIMARY KEY(source_id, event_source, event_id)
);`)
	return err
}

func (s *Store) BeginRun(ctx context.Context, sourceID, kind string) (int64, error) {
	result, err := s.db.ExecContext(ctx, "INSERT INTO runs(source_id,kind,started_at) VALUES(?,?,?)", sourceID, kind, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}
func (s *Store) FinishRun(ctx context.Context, runID int64, complete bool) error {
	_, err := s.db.ExecContext(ctx, "UPDATE runs SET finished_at=?, complete=? WHERE id=?", time.Now().UTC().Format(time.RFC3339Nano), boolInt(complete), runID)
	return err
}

func (s *Store) Get(ctx context.Context, sourceID, kind, resourceID string) (Resource, bool, error) {
	return s.scanOne(ctx, "WHERE source_id=? AND kind=? AND resource_id=?", sourceID, kind, resourceID)
}
func (s *Store) GetByURL(ctx context.Context, sourceID, canonicalURL string) (Resource, bool, error) {
	return s.scanOne(ctx, "WHERE source_id=? AND canonical_url=?", sourceID, canonicalURL)
}
func (s *Store) scanOne(ctx context.Context, where string, args ...any) (Resource, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT source_id,kind,resource_id,canonical_url,record_id,normalized_json,normalized_hash,profile_digest,record_hash,last_seen_run,consecutive_misses,status,last_error,deleted FROM resources `+where, args...)
	resource, err := scanResource(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Resource{}, false, nil
	}
	return resource, err == nil, err
}
func scanResource(row *sql.Row) (Resource, error) {
	var r Resource
	var deleted int
	err := row.Scan(&r.SourceID, &r.Kind, &r.ResourceID, &r.CanonicalURL, &r.RecordID, &r.NormalizedJSON, &r.NormalizedHash, &r.ProfileDigest, &r.RecordHash, &r.LastSeenRun, &r.ConsecutiveMisses, &r.Status, &r.LastError, &deleted)
	r.Deleted = deleted != 0
	return r, err
}

func (s *Store) Save(ctx context.Context, resource Resource) error {
	if resource.Status == "" {
		resource.Status = "active"
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO resources(source_id,kind,resource_id,canonical_url,record_id,normalized_json,normalized_hash,profile_digest,record_hash,last_seen_run,consecutive_misses,status,last_error,deleted,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source_id,kind,resource_id) DO UPDATE SET canonical_url=excluded.canonical_url,record_id=excluded.record_id,normalized_json=excluded.normalized_json,normalized_hash=excluded.normalized_hash,profile_digest=excluded.profile_digest,record_hash=excluded.record_hash,last_seen_run=excluded.last_seen_run,consecutive_misses=excluded.consecutive_misses,status=excluded.status,last_error=excluded.last_error,deleted=excluded.deleted,updated_at=excluded.updated_at`, resource.SourceID, resource.Kind, resource.ResourceID, resource.CanonicalURL, resource.RecordID, resource.NormalizedJSON, resource.NormalizedHash, resource.ProfileDigest, resource.RecordHash, resource.LastSeenRun, resource.ConsecutiveMisses, resource.Status, resource.LastError, boolInt(resource.Deleted), time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) CandidatesForDeletion(ctx context.Context, sourceID, kind string, runID int64, threshold int) ([]Resource, error) {
	if _, err := s.db.ExecContext(ctx, `UPDATE resources SET consecutive_misses=consecutive_misses+1, updated_at=? WHERE source_id=? AND kind=? AND deleted=0 AND last_seen_run < ?`, time.Now().UTC().Format(time.RFC3339Nano), sourceID, kind, runID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT source_id,kind,resource_id,canonical_url,record_id,normalized_json,normalized_hash,profile_digest,record_hash,last_seen_run,consecutive_misses,status,last_error,deleted FROM resources WHERE source_id=? AND kind=? AND deleted=0 AND consecutive_misses>=?`, sourceID, kind, threshold)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Resource
	for rows.Next() {
		var r Resource
		var deleted int
		if err := rows.Scan(&r.SourceID, &r.Kind, &r.ResourceID, &r.CanonicalURL, &r.RecordID, &r.NormalizedJSON, &r.NormalizedHash, &r.ProfileDigest, &r.RecordHash, &r.LastSeenRun, &r.ConsecutiveMisses, &r.Status, &r.LastError, &deleted); err != nil {
			return nil, err
		}
		r.Deleted = deleted != 0
		result = append(result, r)
	}
	return result, rows.Err()
}
func (s *Store) MarkDeleted(ctx context.Context, sourceID, kind, resourceID string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE resources SET deleted=1,status='deleted',updated_at=? WHERE source_id=? AND kind=? AND resource_id=?", time.Now().UTC().Format(time.RFC3339Nano), sourceID, kind, resourceID)
	return err
}
func (s *Store) Active(ctx context.Context) ([]Resource, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT source_id,kind,resource_id,canonical_url,record_id,normalized_json,normalized_hash,profile_digest,record_hash,last_seen_run,consecutive_misses,status,last_error,deleted FROM resources WHERE deleted=0 AND status='active'`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Resource
	for rows.Next() {
		var r Resource
		var deleted int
		if err := rows.Scan(&r.SourceID, &r.Kind, &r.ResourceID, &r.CanonicalURL, &r.RecordID, &r.NormalizedJSON, &r.NormalizedHash, &r.ProfileDigest, &r.RecordHash, &r.LastSeenRun, &r.ConsecutiveMisses, &r.Status, &r.LastError, &deleted); err != nil {
			return nil, err
		}
		r.Deleted = deleted != 0
		result = append(result, r)
	}
	return result, rows.Err()
}
func (s *Store) ActiveFor(ctx context.Context, sourceID, kind string) ([]Resource, error) {
	query := `SELECT source_id,kind,resource_id,canonical_url,record_id,normalized_json,normalized_hash,profile_digest,record_hash,last_seen_run,consecutive_misses,status,last_error,deleted FROM resources WHERE deleted=0 AND status='active'`
	args := []any{}
	if sourceID != "" {
		query += " AND source_id=?"
		args = append(args, sourceID)
	}
	if kind != "" {
		query += " AND kind=?"
		args = append(args, kind)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Resource
	for rows.Next() {
		var r Resource
		var deleted int
		if err := rows.Scan(&r.SourceID, &r.Kind, &r.ResourceID, &r.CanonicalURL, &r.RecordID, &r.NormalizedJSON, &r.NormalizedHash, &r.ProfileDigest, &r.RecordHash, &r.LastSeenRun, &r.ConsecutiveMisses, &r.Status, &r.LastError, &deleted); err != nil {
			return nil, err
		}
		r.Deleted = deleted != 0
		result = append(result, r)
	}
	return result, rows.Err()
}
func (s *Store) Failures(ctx context.Context) ([]Resource, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT source_id,kind,resource_id,canonical_url,record_id,normalized_json,normalized_hash,profile_digest,record_hash,last_seen_run,consecutive_misses,status,last_error,deleted FROM resources WHERE status='failed' ORDER BY updated_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Resource
	for rows.Next() {
		var r Resource
		var deleted int
		if err := rows.Scan(&r.SourceID, &r.Kind, &r.ResourceID, &r.CanonicalURL, &r.RecordID, &r.NormalizedJSON, &r.NormalizedHash, &r.ProfileDigest, &r.RecordHash, &r.LastSeenRun, &r.ConsecutiveMisses, &r.Status, &r.LastError, &deleted); err != nil {
			return nil, err
		}
		r.Deleted = deleted != 0
		result = append(result, r)
	}
	return result, rows.Err()
}
func (s *Store) Failure(ctx context.Context, sourceID, kind, resourceID, detail string) error {
	_, err := s.db.ExecContext(ctx, "UPDATE resources SET status='failed',last_error=?,updated_at=? WHERE source_id=? AND kind=? AND resource_id=?", detail, time.Now().UTC().Format(time.RFC3339Nano), sourceID, kind, resourceID)
	return err
}

// EnqueueEvent returns false for a duplicate CloudEvent ID.
func (s *Store) EnqueueEvent(ctx context.Context, event Event) (bool, error) {
	if event.Status == "" {
		event.Status = "queued"
	}
	result, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO events(source_id,event_source,event_id,subject,event_type,payload,status,last_error,updated_at) VALUES(?,?,?,?,?,?,?,?,?)`, event.SourceID, event.EventSource, event.EventID, event.Subject, event.Type, event.Payload, event.Status, event.LastError, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	if err != nil || affected == 1 {
		return affected == 1, err
	}
	var status string
	if err := s.db.QueryRowContext(ctx, "SELECT status FROM events WHERE source_id=? AND event_source=? AND event_id=?", event.SourceID, event.EventSource, event.EventID).Scan(&status); err != nil {
		return false, err
	}
	if status == "complete" {
		return false, nil
	}
	_, err = s.db.ExecContext(ctx, "UPDATE events SET status='queued',updated_at=? WHERE source_id=? AND event_source=? AND event_id=?", time.Now().UTC().Format(time.RFC3339Nano), event.SourceID, event.EventSource, event.EventID)
	return err == nil, err
}
func (s *Store) CompleteEvent(ctx context.Context, event Event, err error) error {
	status := "complete"
	detail := ""
	if err != nil {
		status = "failed"
		detail = err.Error()
	}
	_, execErr := s.db.ExecContext(ctx, "UPDATE events SET status=?,last_error=?,updated_at=? WHERE source_id=? AND event_source=? AND event_id=?", status, detail, time.Now().UTC().Format(time.RFC3339Nano), event.SourceID, event.EventSource, event.EventID)
	return execErr
}

func (s *Store) RetryableEvents(ctx context.Context) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT source_id,event_source,event_id,subject,event_type,payload,status,last_error FROM events WHERE status IN ('queued','failed') ORDER BY updated_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []Event
	for rows.Next() {
		var event Event
		if err := rows.Scan(&event.SourceID, &event.EventSource, &event.EventID, &event.Subject, &event.Type, &event.Payload, &event.Status, &event.LastError); err != nil {
			return nil, err
		}
		result = append(result, event)
	}
	return result, rows.Err()
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}
