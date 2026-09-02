package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valyala/fastjson"
)

// ErrReplicationNotFound is returned when a cancel targets an unknown replication.
var ErrReplicationNotFound = errors.New("replication_not_found")

// ReplicationRequest mirrors the CouchDB POST /_replicate body.
//
// source and target are either a bare string (a local database name or a
// remote kdb3 URL such as "http://host:8001/db") or an object of the form
// {"url":"http://host:8001/db","token":"…","headers":{"X":"Y"}} /
// {"name":"localdb"}.
type ReplicationRequest struct {
	Source        json.RawMessage `json:"source"`
	Target        json.RawMessage `json:"target"`
	Continuous    bool            `json:"continuous"`
	CreateTarget  bool            `json:"create_target"`
	Cancel        bool            `json:"cancel"`
	ReplicationID string          `json:"replication_id"`
	SinceSeq      int64           `json:"since_seq"`
}

// ReplicationResult is the POST /_replicate response.
type ReplicationResult struct {
	OK               bool   `json:"ok"`
	ReplicationID    string `json:"replication_id"`
	SessionID        string `json:"session_id,omitempty"`
	Continuous       bool   `json:"continuous,omitempty"`
	Cancelled        bool   `json:"cancelled,omitempty"`
	SourceLastSeq    int64  `json:"source_last_seq,omitempty"`
	DocsRead         int64  `json:"docs_read"`
	DocsWritten      int64  `json:"docs_written"`
	DocWriteFailures int64  `json:"doc_write_failures"`
	NoOps            int64  `json:"no_ops,omitempty"`
}

// repStats holds live counters for a replication (safe for concurrent use).
type repStats struct {
	docsRead     atomic.Int64
	docsWritten  atomic.Int64
	writeFailure atomic.Int64
	noOps        atomic.Int64
	sourceSeq    atomic.Int64
}

// endpointSpec is the parsed source/target descriptor.
type endpointSpec struct {
	display string // sanitized, for ids / listing (never contains a token)
	dbName  string

	remote  bool
	baseURL string
	header  http.Header
}

// parseEndpointSpec accepts the CouchDB string or object endpoint form.
func parseEndpointSpec(raw json.RawMessage) (endpointSpec, error) {
	if len(raw) == 0 {
		return endpointSpec{}, fmt.Errorf("%w: endpoint is required", ErrDocumentInvalidInput)
	}

	trimmed := strings.TrimSpace(string(raw))
	if strings.HasPrefix(trimmed, "\"") {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return endpointSpec{}, fmt.Errorf("%w: %s", ErrBadJSON, err.Error())
		}
		return endpointFromString(s, nil, "")
	}

	var obj struct {
		URL     string            `json:"url"`
		Name    string            `json:"name"`
		Token   string            `json:"token"`
		Headers map[string]string `json:"headers"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return endpointSpec{}, fmt.Errorf("%w: %s", ErrBadJSON, err.Error())
	}
	header := http.Header{}
	for k, v := range obj.Headers {
		header.Set(k, v)
	}
	if obj.Token != "" {
		header.Set("Authorization", "Bearer "+obj.Token)
	}
	if obj.URL != "" {
		return endpointFromString(obj.URL, header, obj.Token)
	}
	if obj.Name != "" {
		return endpointFromString(obj.Name, header, "")
	}
	return endpointSpec{}, fmt.Errorf("%w: endpoint needs a url or name", ErrDocumentInvalidInput)
}

func endpointFromString(s string, header http.Header, token string) (endpointSpec, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return endpointSpec{}, fmt.Errorf("%w: empty endpoint", ErrDocumentInvalidInput)
	}

	if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
		u, err := url.Parse(s)
		if err != nil {
			return endpointSpec{}, fmt.Errorf("%w: %s", ErrDocumentInvalidInput, err.Error())
		}
		path := strings.Trim(u.Path, "/")
		if path == "" {
			return endpointSpec{}, fmt.Errorf("%w: remote endpoint must include a database name", ErrDocumentInvalidInput)
		}
		segs := strings.Split(path, "/")
		dbName := segs[len(segs)-1]
		base := *u
		base.Path = "/" + strings.Join(segs[:len(segs)-1], "/")
		base.RawQuery = ""
		base.Fragment = ""
		if header == nil {
			header = http.Header{}
		}
		// Pull a token out of URL userinfo (http://token@host/db) if present.
		if token == "" && u.User != nil {
			if pw, ok := u.User.Password(); ok && pw != "" {
				header.Set("Authorization", "Bearer "+pw)
			} else if u.User.Username() != "" {
				header.Set("Authorization", "Bearer "+u.User.Username())
			}
			base.User = nil
		}
		display := strings.TrimRight(base.String(), "/") + "/" + dbName
		return endpointSpec{
			display: display,
			dbName:  dbName,
			remote:  true,
			baseURL: strings.TrimRight(base.String(), "/"),
			header:  header,
		}, nil
	}

	if !ValidateDatabaseName(s) {
		return endpointSpec{}, fmt.Errorf("%w: %s", ErrDatabaseInvalidName, s)
	}
	return endpointSpec{display: s, dbName: s}, nil
}

// repEndpoint is the read/write surface a replication needs. Both local
// databases and remote kdb3 servers implement it.
type repEndpoint interface {
	describe() string
	ensure(create bool) error
	lastSeq() (int64, error)
	changes(since int64, limit int) ([]Change, error)
	// getDoc returns the stored body (including _id/_rev) for a live doc.
	getDoc(id string) (body []byte, found bool, err error)
	// currentVersion reports the target's live revision for id (found=false
	// when the doc is missing or a tombstone we cannot resurrect).
	currentVersion(id string) (version int, found bool, err error)
	// writeDoc applies a create/update/delete to the target. fields is the
	// document body with _id/_rev/_deleted stripped (nil for a delete).
	writeDoc(id string, fields []byte, deleted bool) (written bool, err error)
	// changeSignal returns a channel that closes when new changes may exist.
	// It must be captured before reading changes so writes are not missed.
	changeSignal() <-chan struct{}
}

// ---- local endpoint ----

type localEndpoint struct {
	kdb    *KDB
	dbName string
}

func (e *localEndpoint) describe() string { return e.dbName }

func (e *localEndpoint) ensure(create bool) error {
	if _, err := e.kdb.DBStat(e.dbName); err == nil {
		return nil
	}
	if !create {
		return ErrDatabaseNotFound
	}
	return e.kdb.Open(e.dbName, true)
}

func (e *localEndpoint) lastSeq() (int64, error) {
	stat, err := e.kdb.DBStat(e.dbName)
	if err != nil {
		return 0, err
	}
	return stat.UpdateSeq, nil
}

func (e *localEndpoint) changes(since int64, limit int) ([]Change, error) {
	return e.kdb.ChangesRows(e.dbName, since, limit, false)
}

func (e *localEndpoint) getDoc(id string) ([]byte, bool, error) {
	doc, err := e.kdb.GetDocument(e.dbName, &Document{ID: id}, true)
	if err != nil {
		if errors.Is(err, ErrDocumentNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return doc.Data, true, nil
}

func (e *localEndpoint) currentVersion(id string) (int, bool, error) {
	version, found, _, err := e.kdb.DocumentMeta(e.dbName, id)
	if err != nil {
		return 0, false, err
	}
	// A tombstone still carries a version we can resurrect against locally.
	return version, found, nil
}

func (e *localEndpoint) writeDoc(id string, fields []byte, deleted bool) (bool, error) {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		version, found, tombstoned, err := e.kdb.DocumentMeta(e.dbName, id)
		if err != nil {
			return false, err
		}
		if deleted {
			if !found || tombstoned {
				return false, nil // nothing live to delete
			}
			doc := &Document{ID: id, Version: version, Deleted: true}
			if _, err := e.kdb.PutDocument(e.dbName, doc); err != nil {
				if errors.Is(err, ErrDocumentConflict) {
					lastErr = err
					continue
				}
				return false, err
			}
			return true, nil
		}

		doc, err := ParseDocument(fields)
		if err != nil {
			return false, err
		}
		doc.ID = id
		doc.Deleted = false
		if found || tombstoned {
			doc.Version = version
		} else {
			doc.Version = 0
		}
		if _, err := e.kdb.PutDocument(e.dbName, doc); err != nil {
			if errors.Is(err, ErrDocumentConflict) {
				lastErr = err
				continue
			}
			return false, err
		}
		return true, nil
	}
	if lastErr == nil {
		lastErr = ErrDocumentConflict
	}
	return false, lastErr
}

func (e *localEndpoint) changeSignal() <-chan struct{} {
	ch, err := e.kdb.ChangesNotifyChan(e.dbName)
	if err != nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return ch
}

// ---- remote endpoint ----

const remotePollInterval = time.Second

type remoteEndpoint struct {
	client  *http.Client
	baseURL string
	dbName  string
	header  http.Header
	display string
}

func (e *remoteEndpoint) describe() string { return e.display }

func (e *remoteEndpoint) docURL(id string) string {
	if strings.HasPrefix(id, "_design/") {
		rest := strings.TrimPrefix(id, "_design/")
		return e.baseURL + "/" + e.dbName + "/_design/" + url.PathEscape(rest)
	}
	return e.baseURL + "/" + e.dbName + "/" + url.PathEscape(id)
}

func (e *remoteEndpoint) newRequest(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	for k, vs := range e.header {
		for _, v := range vs {
			req.Header.Set(k, v)
		}
	}
	return req, nil
}

func (e *remoteEndpoint) ensure(create bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := e.newRequest(ctx, http.MethodGet, e.baseURL+"/"+e.dbName, nil)
	if err != nil {
		return err
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	if !create {
		return ErrDatabaseNotFound
	}
	req, err = e.newRequest(ctx, http.MethodPut, e.baseURL+"/"+e.dbName, nil)
	if err != nil {
		return err
	}
	resp, err = e.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusPreconditionFailed {
		return nil
	}
	return fmt.Errorf("%w: create target %s returned %d", ErrInternalError, e.display, resp.StatusCode)
}

func (e *remoteEndpoint) lastSeq() (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := e.newRequest(ctx, http.MethodGet, e.baseURL+"/"+e.dbName, nil)
	if err != nil {
		return 0, err
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("%w: source %s returned %d", ErrDatabaseNotFound, e.display, resp.StatusCode)
	}
	var stat DatabaseStat
	if err := json.NewDecoder(resp.Body).Decode(&stat); err != nil {
		return 0, err
	}
	return stat.UpdateSeq, nil
}

func (e *remoteEndpoint) changes(since int64, limit int) ([]Change, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	u := fmt.Sprintf("%s/%s/_changes?since=%d&limit=%d", e.baseURL, e.dbName, since, limit)
	req, err := e.newRequest(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: source changes returned %d", ErrInternalError, resp.StatusCode)
	}
	var result ChangesResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.Results, nil
}

func (e *remoteEndpoint) getDoc(id string) ([]byte, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := e.newRequest(ctx, http.MethodGet, e.docURL(id), nil)
	if err != nil {
		return nil, false, err
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("%w: get doc returned %d", ErrInternalError, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, false, err
	}
	return body, true, nil
}

func (e *remoteEndpoint) currentVersion(id string) (int, bool, error) {
	// GET (not HEAD): HEAD has no route for _design/ docs, and GET returns the
	// _rev in the body for both regular and design documents.
	body, found, err := e.getDoc(id)
	if err != nil || !found {
		return 0, found, err
	}
	var p fastjson.Parser
	v, perr := p.ParseBytes(body)
	if perr != nil {
		return 0, false, fmt.Errorf("%w: %s", ErrBadJSON, perr.Error())
	}
	version := v.GetInt("_rev")
	return version, true, nil
}

func (e *remoteEndpoint) writeDoc(id string, fields []byte, deleted bool) (bool, error) {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		version, found, err := e.currentVersion(id)
		if err != nil {
			return false, err
		}
		if deleted {
			if !found {
				return false, nil
			}
			ok, conflict, err := e.deleteRemote(id, version)
			if err != nil {
				return false, err
			}
			if conflict {
				lastErr = ErrDocumentConflict
				continue
			}
			return ok, nil
		}

		body := prependDocMeta(id, version, fields)
		ok, conflict, err := e.putRemote(id, body)
		if err != nil {
			return false, err
		}
		if conflict {
			lastErr = ErrDocumentConflict
			continue
		}
		return ok, nil
	}
	if lastErr == nil {
		lastErr = ErrDocumentConflict
	}
	return false, lastErr
}

func (e *remoteEndpoint) putRemote(id string, body []byte) (ok bool, conflict bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := e.newRequest(ctx, http.MethodPut, e.docURL(id), strings.NewReader(string(body)))
	if err != nil {
		return false, false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		return false, false, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		return true, false, nil
	case http.StatusConflict:
		return false, true, nil
	default:
		return false, false, fmt.Errorf("%w: put doc returned %d", ErrInternalError, resp.StatusCode)
	}
}

func (e *remoteEndpoint) deleteRemote(id string, version int) (ok bool, conflict bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	u := e.docURL(id) + "?rev=" + strconv.Itoa(version)
	req, err := e.newRequest(ctx, http.MethodDelete, u, nil)
	if err != nil {
		return false, false, err
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return false, false, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, false, nil
	case http.StatusConflict:
		return false, true, nil
	case http.StatusNotFound:
		return false, false, nil
	default:
		return false, false, fmt.Errorf("%w: delete doc returned %d", ErrInternalError, resp.StatusCode)
	}
}

func (e *remoteEndpoint) changeSignal() <-chan struct{} {
	ch := make(chan struct{})
	time.AfterFunc(remotePollInterval, func() { close(ch) })
	return ch
}

// stripMeta removes _id/_rev/_deleted, returning the bare field object so two
// documents can be compared for content equality regardless of revision.
func stripMeta(body []byte) ([]byte, error) {
	if len(body) == 0 {
		return []byte("{}"), nil
	}
	var p fastjson.Parser
	v, err := p.ParseBytes(body)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrBadJSON, err.Error())
	}
	if v.Type() != fastjson.TypeObject {
		return nil, fmt.Errorf("%w: document is not an object", ErrDocumentInvalidInput)
	}
	v.Del("_id")
	v.Del("_rev")
	v.Del("_deleted")
	return v.MarshalTo(nil), nil
}
