package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// changesBatchLimit bounds how many changes are pulled per round.
const changesBatchLimit = 500

// replicationJob tracks a single running replication.
type replicationJob struct {
	id         string
	source     string
	target     string
	continuous bool
	createdAt  time.Time
	stats      *repStats
	cancel     context.CancelFunc
	done       chan struct{}
}

// ReplicationManager owns active replications for a KDB instance.
type ReplicationManager struct {
	kdb    *KDB
	client *http.Client

	mu   sync.Mutex
	jobs map[string]*replicationJob
}

// NewReplicationManager builds a manager bound to a KDB instance.
func NewReplicationManager(kdb *KDB) *ReplicationManager {
	return &ReplicationManager{
		kdb:    kdb,
		client: &http.Client{Timeout: 0},
		jobs:   make(map[string]*replicationJob),
	}
}

func (m *ReplicationManager) buildEndpoint(spec endpointSpec) repEndpoint {
	if spec.remote {
		return &remoteEndpoint{
			client:  m.client,
			baseURL: spec.baseURL,
			dbName:  spec.dbName,
			header:  spec.header,
			display: spec.display,
		}
	}
	return &localEndpoint{kdb: m.kdb, dbName: spec.dbName}
}

func replicationID(source, target string, continuous bool) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%t", source, target, continuous)))
	return hex.EncodeToString(h[:])[:32]
}

// Replicate runs a one-shot replication, starts a continuous one, or cancels.
func (m *ReplicationManager) Replicate(req ReplicationRequest) (*ReplicationResult, error) {
	if req.Cancel {
		return m.cancel(req)
	}

	sourceSpec, err := parseEndpointSpec(req.Source)
	if err != nil {
		return nil, err
	}
	targetSpec, err := parseEndpointSpec(req.Target)
	if err != nil {
		return nil, err
	}

	id := replicationID(sourceSpec.display, targetSpec.display, req.Continuous)
	source := m.buildEndpoint(sourceSpec)
	target := m.buildEndpoint(targetSpec)

	if err := target.ensure(req.CreateTarget); err != nil {
		return nil, err
	}
	if _, err := source.lastSeq(); err != nil {
		return nil, err
	}

	if req.Continuous {
		return m.startContinuous(id, sourceSpec, targetSpec, source, target, req.SinceSeq)
	}

	stats := &repStats{}
	stats.sourceSeq.Store(req.SinceSeq)
	if _, err := m.replicateOnce(context.Background(), source, target, stats); err != nil {
		return nil, err
	}
	return &ReplicationResult{
		OK:               true,
		ReplicationID:    id,
		SessionID:        newSessionID(),
		SourceLastSeq:    stats.sourceSeq.Load(),
		DocsRead:         stats.docsRead.Load(),
		DocsWritten:      stats.docsWritten.Load(),
		DocWriteFailures: stats.writeFailure.Load(),
		NoOps:            stats.noOps.Load(),
	}, nil
}

func (m *ReplicationManager) startContinuous(id string, srcSpec, tgtSpec endpointSpec, source, target repEndpoint, since int64) (*ReplicationResult, error) {
	m.mu.Lock()
	if existing, ok := m.jobs[id]; ok {
		m.mu.Unlock()
		return &ReplicationResult{OK: true, ReplicationID: existing.id, Continuous: true}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	stats := &repStats{}
	stats.sourceSeq.Store(since)
	job := &replicationJob{
		id:         id,
		source:     srcSpec.display,
		target:     tgtSpec.display,
		continuous: true,
		createdAt:  time.Now(),
		stats:      stats,
		cancel:     cancel,
		done:       make(chan struct{}),
	}
	m.jobs[id] = job
	m.mu.Unlock()

	go func() {
		defer close(job.done)
		defer func() {
			m.mu.Lock()
			delete(m.jobs, id)
			m.mu.Unlock()
		}()
		m.runContinuous(ctx, source, target, stats)
	}()

	return &ReplicationResult{OK: true, ReplicationID: id, Continuous: true}, nil
}

func (m *ReplicationManager) cancel(req ReplicationRequest) (*ReplicationResult, error) {
	id := req.ReplicationID
	if id == "" {
		sourceSpec, err := parseEndpointSpec(req.Source)
		if err != nil {
			return nil, err
		}
		targetSpec, err := parseEndpointSpec(req.Target)
		if err != nil {
			return nil, err
		}
		id = replicationID(sourceSpec.display, targetSpec.display, true)
	}

	m.mu.Lock()
	job, ok := m.jobs[id]
	m.mu.Unlock()
	if !ok {
		return nil, ErrReplicationNotFound
	}
	job.cancel()
	<-job.done
	return &ReplicationResult{OK: true, ReplicationID: id, Cancelled: true}, nil
}

// runContinuous subscribes to the source's changes and keeps replicating until
// the context is cancelled.
func (m *ReplicationManager) runContinuous(ctx context.Context, source, target repEndpoint, stats *repStats) {
	const maxIdle = 30 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		// Capture the change signal before reading so a write that lands
		// mid-batch is not missed.
		signal := source.changeSignal()
		n, err := m.replicateOnce(ctx, source, target, stats)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(remotePollInterval):
			}
			continue
		}
		if n > 0 {
			continue // keep draining while changes are flowing
		}
		select {
		case <-ctx.Done():
			return
		case <-signal:
		case <-time.After(maxIdle):
		}
	}
}

// replicateOnce pulls and applies every pending change from source to target,
// advancing the checkpoint. It returns the number of changes processed.
func (m *ReplicationManager) replicateOnce(ctx context.Context, source, target repEndpoint, stats *repStats) (int, error) {
	processed := 0
	for {
		if ctx.Err() != nil {
			return processed, ctx.Err()
		}
		since := stats.sourceSeq.Load()
		changes, err := source.changes(since, changesBatchLimit)
		if err != nil {
			return processed, err
		}
		if len(changes) == 0 {
			return processed, nil
		}
		for _, ch := range changes {
			if ctx.Err() != nil {
				return processed, ctx.Err()
			}
			if err := m.applyChange(source, target, ch, stats); err != nil {
				return processed, err
			}
			if ch.UpdateSeq > stats.sourceSeq.Load() {
				stats.sourceSeq.Store(ch.UpdateSeq)
			}
			processed++
		}
		if len(changes) < changesBatchLimit {
			return processed, nil
		}
	}
}

// applyChange replicates a single change row to the target.
func (m *ReplicationManager) applyChange(source, target repEndpoint, ch Change, stats *repStats) error {
	stats.docsRead.Add(1)

	if ch.Deleted {
		written, err := target.writeDoc(ch.ID, nil, true)
		if err != nil {
			stats.writeFailure.Add(1)
			return nil // surface via counters; keep the stream alive
		}
		if written {
			stats.docsWritten.Add(1)
		} else {
			stats.noOps.Add(1)
		}
		return nil
	}

	body, found, err := source.getDoc(ch.ID)
	if err != nil {
		stats.writeFailure.Add(1)
		return nil
	}
	if !found {
		// Raced with a delete on the source; treat as delete.
		written, err := target.writeDoc(ch.ID, nil, true)
		if err == nil && written {
			stats.docsWritten.Add(1)
		} else {
			stats.noOps.Add(1)
		}
		return nil
	}

	fields, err := stripMeta(body)
	if err != nil {
		stats.writeFailure.Add(1)
		return nil
	}

	// Skip when the target already holds identical content (also breaks
	// bidirectional replication echo loops).
	if _, tfound, verr := target.currentVersion(ch.ID); verr == nil && tfound {
		if tbody, tf, gerr := target.getDoc(ch.ID); gerr == nil && tf {
			if tfields, serr := stripMeta(tbody); serr == nil && bytes.Equal(fields, tfields) {
				stats.noOps.Add(1)
				return nil
			}
		}
	}

	written, err := target.writeDoc(ch.ID, fields, false)
	if err != nil {
		stats.writeFailure.Add(1)
		return nil
	}
	if written {
		stats.docsWritten.Add(1)
	} else {
		stats.noOps.Add(1)
	}
	return nil
}

// ActiveTasks lists running continuous replications (CouchDB _active_tasks).
func (m *ReplicationManager) ActiveTasks() []map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	tasks := make([]map[string]any, 0, len(m.jobs))
	for _, job := range m.jobs {
		tasks = append(tasks, map[string]any{
			"type":                    "replication",
			"replication_id":          job.id,
			"continuous":              job.continuous,
			"source":                  job.source,
			"target":                  job.target,
			"docs_read":               job.stats.docsRead.Load(),
			"docs_written":            job.stats.docsWritten.Load(),
			"doc_write_failures":      job.stats.writeFailure.Load(),
			"checkpointed_source_seq": job.stats.sourceSeq.Load(),
			"started_on":              job.createdAt.Unix(),
		})
	}
	return tasks
}

// Shutdown cancels every running replication and waits for them to stop.
func (m *ReplicationManager) Shutdown() {
	m.mu.Lock()
	jobs := make([]*replicationJob, 0, len(m.jobs))
	for _, job := range m.jobs {
		jobs = append(jobs, job)
	}
	m.mu.Unlock()
	for _, job := range jobs {
		job.cancel()
		<-job.done
	}
}

func newSessionID() string {
	return hex.EncodeToString(randomBytes(16))
}
