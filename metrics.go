package main

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kdb_http_requests_total",
		Help: "Total number of HTTP requests handled by kdb3.",
	}, []string{"method", "route", "code"})

	httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kdb_http_request_duration_seconds",
		Help:    "HTTP request duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route"})

	databasesOpen = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kdb_databases_open",
		Help: "Number of open databases.",
	})

	databaseDocCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kdb_database_doc_count",
		Help: "Number of live documents in a database.",
	}, []string{"db"})

	databaseDeletedDocCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kdb_database_deleted_doc_count",
		Help: "Number of deleted documents in a database.",
	}, []string{"db"})

	databaseUpdateSeq = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kdb_database_update_seq",
		Help: "Current update sequence of a database.",
	}, []string{"db"})

	documentsWrittenTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kdb_documents_written_total",
		Help: "Total document write attempts.",
	}, []string{"db", "result"})

	documentsReadTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kdb_documents_read_total",
		Help: "Total document read attempts.",
	}, []string{"db", "result"})

	documentWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kdb_document_write_duration_seconds",
		Help:    "Document write duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"db"})

	documentReadDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kdb_document_read_duration_seconds",
		Help:    "Document read duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"db"})

	viewQueriesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kdb_view_queries_total",
		Help: "Total view query attempts.",
	}, []string{"db", "result"})

	viewBuildDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kdb_view_build_duration_seconds",
		Help:    "View build duration in seconds when a build runs.",
		Buckets: prometheus.DefBuckets,
	}, []string{"db"})

	viewsOpen = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kdb_views_open",
		Help: "Number of open views per database.",
	}, []string{"db"})

	vacuumInProgress = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kdb_vacuum_in_progress",
		Help: "Whether a vacuum is in progress for a database (1 or 0).",
	}, []string{"db"})

	vacuumTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kdb_vacuum_total",
		Help: "Total vacuum attempts.",
	}, []string{"db", "result"})

	vacuumDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kdb_vacuum_duration_seconds",
		Help:    "Vacuum duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"db"})

	dbReaderPoolInUse = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kdb_db_reader_pool_in_use",
		Help: "Number of database readers currently checked out.",
	}, []string{"db"})

	dbWriterInUse = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kdb_db_writer_in_use",
		Help: "Whether the database writer is checked out (1 or 0).",
	}, []string{"db"})

	viewReaderPoolInUse = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kdb_view_reader_pool_in_use",
		Help: "Number of view readers currently checked out, aggregated per database.",
	}, []string{"db"})
)

func metricsResult(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, ErrDocumentConflict) {
		return "conflict"
	}
	if errors.Is(err, ErrDocumentNotFound) {
		return "not_found"
	}
	return "error"
}

func syncDatabaseStatGauges(db *DefaultDatabase) {
	name := db.Name
	databaseDocCount.WithLabelValues(name).Set(float64(db.docCount.Load()))
	databaseDeletedDocCount.WithLabelValues(name).Set(float64(db.deletedCount.Load()))
	databaseUpdateSeq.WithLabelValues(name).Set(float64(db.updateSeq.Load()))
}

func clearDatabaseStatGauges(name string) {
	databaseDocCount.DeleteLabelValues(name)
	databaseDeletedDocCount.DeleteLabelValues(name)
	databaseUpdateSeq.DeleteLabelValues(name)
	vacuumInProgress.DeleteLabelValues(name)
	dbReaderPoolInUse.DeleteLabelValues(name)
	dbWriterInUse.DeleteLabelValues(name)
	viewReaderPoolInUse.DeleteLabelValues(name)
	viewsOpen.DeleteLabelValues(name)
}

func syncDBPoolGauges(db *DefaultDatabase) {
	name := db.Name
	dbReaderPoolInUse.WithLabelValues(name).Set(float64(cap(db.reader) - len(db.reader)))
	dbWriterInUse.WithLabelValues(name).Set(float64(cap(db.writer) - len(db.writer)))
}

func syncViewPoolGauges(mgr *DefaultViewManager) {
	mgr.rwMutex.RLock()
	defer mgr.rwMutex.RUnlock()
	syncViewPoolGaugesLocked(mgr)
}

// syncViewPoolGaugesLocked updates view gauges; caller must hold mgr.rwMutex (R or W).
func syncViewPoolGaugesLocked(mgr *DefaultViewManager) {
	viewsOpen.WithLabelValues(mgr.DBName).Set(float64(len(mgr.views)))
	inUse := 0
	for _, v := range mgr.views {
		inUse += cap(v.viewReader) - len(v.viewReader)
	}
	viewReaderPoolInUse.WithLabelValues(mgr.DBName).Set(float64(inUse))
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (sr *statusRecorder) WriteHeader(code int) {
	sr.code = code
	sr.ResponseWriter.WriteHeader(code)
}

// Flush implements http.Flusher when the underlying writer supports it (SSE).
func (sr *statusRecorder) Flush() {
	if f, ok := sr.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap exposes the underlying ResponseWriter for http.ResponseController.
func (sr *statusRecorder) Unwrap() http.ResponseWriter {
	return sr.ResponseWriter
}

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" ||
			strings.HasPrefix(r.URL.Path, "/_utils") ||
			strings.HasPrefix(r.URL.Path, "/_docs") {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		sr := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(sr, r)

		route := "unknown"
		if current := mux.CurrentRoute(r); current != nil {
			if name := current.GetName(); name != "" {
				route = name
			}
		}
		code := strconv.Itoa(sr.code)
		httpRequestsTotal.WithLabelValues(r.Method, route, code).Inc()
		httpRequestDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
	})
}
