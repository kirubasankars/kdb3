package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"kdb3/sqlite3"
)

var viewSQLInvalidKeywords = []string{
	"PRAGMA", "ALTER", "ATTACH", "TRANSACTION", "DETACH", "DROP", "EXPLAIN", "REINDEX", "SAVEPOINT", "VACUUM",
}

const defaultAtelierLimit = 300

// ViewAtelierSQLError is a statement-level diagnostic from dry-run or validation.
type ViewAtelierSQLError struct {
	Phase   string `json:"phase"`
	Index   int    `json:"index"`
	SQL     string `json:"sql,omitempty"`
	Message string `json:"message"`
}

// ViewAtelierTableCount is a post-run table row count.
type ViewAtelierTableCount struct {
	Name string `json:"name"`
	Rows int64  `json:"rows"`
}

// ViewAtelierDryRunRequest is the body for POST .../_dry_run.
type ViewAtelierDryRunRequest struct {
	Setup      []string          `json:"setup"`
	Run        []string          `json:"run"`
	Select     map[string]string `json:"select"`
	Since      int64             `json:"since"`
	Limit      int               `json:"limit"`
	SelectName string            `json:"select_name"`
	Params     map[string]string `json:"params"`
	IncludeSQL bool              `json:"include_sql"`
}

// ViewAtelierDryRunResult is the structured dry-run response.
type ViewAtelierDryRunResult struct {
	OK             bool                    `json:"ok"`
	Since          int64                   `json:"since"`
	CurrentSeq     int64                   `json:"current_update_seq"`
	NextSeq        int64                   `json:"next_update_seq"`
	DocsInWindow   int64                   `json:"docs_in_window"`
	Tables         []ViewAtelierTableCount `json:"tables,omitempty"`
	Result         json.RawMessage         `json:"result,omitempty"`
	GeneratedSQL   string                  `json:"generated_sql,omitempty"`
	Errors         []ViewAtelierSQLError   `json:"errors"`
	Error          string                  `json:"error,omitempty"`
	Reason         string                  `json:"reason,omitempty"`
}

// ViewStatus is catch-up state for a view vs the docs DB.
type ViewStatus struct {
	DBUpdateSeq   int64 `json:"db_update_seq"`
	ViewUpdateSeq int64 `json:"view_update_seq"`
	Lag           int64 `json:"lag"`
	Open          bool  `json:"open"`
	Built         bool  `json:"built"`
}

func formatSQLStmtError(phase string, index int, message string) string {
	return fmt.Sprintf("%s[%d]: %s", phase, index, message)
}

func formatSQLKeywordError(kw string) string {
	return fmt.Sprintf("invalid keyword: %s", kw)
}

func sqlErrorReason(err ViewAtelierSQLError) string {
	if err.Phase == "keyword" {
		return formatSQLKeywordError(err.Message)
	}
	return formatSQLStmtError(err.Phase, err.Index, err.Message)
}

func checkViewSQLKeywords(setup, run []string, selects map[string]string) *ViewAtelierSQLError {
	check := func(stmts []string) *ViewAtelierSQLError {
		for i, x := range stmts {
			if kw := containsInvalidSQLKeyword(x, viewSQLInvalidKeywords); kw != "" {
				return &ViewAtelierSQLError{Phase: "keyword", Index: i, SQL: x, Message: kw}
			}
		}
		return nil
	}
	if err := check(setup); err != nil {
		return err
	}
	if err := check(run); err != nil {
		return err
	}
	if selects != nil {
		i := 0
		for _, x := range selects {
			if kw := containsInvalidSQLKeyword(x, viewSQLInvalidKeywords); kw != "" {
				return &ViewAtelierSQLError{Phase: "keyword", Index: i, SQL: x, Message: kw}
			}
			i++
		}
	}
	return nil
}

func (mgr *DefaultViewManager) docsDBPath() string {
	fileName := mgr.localDB.GetDatabaseFileName(mgr.DBName)
	return absPath(filepath.Join(mgr.serviceLocator.GetDBDirPath(), fileName+dbExt))
}

// DryRun executes draft view SQL against a docs window in memory (no view file writes).
func (mgr *DefaultViewManager) DryRun(req ViewAtelierDryRunRequest) (*ViewAtelierDryRunResult, error) {
	return runViewAtelierDryRun(mgr.docsDBPath(), mgr, req)
}

// GetViewStatus returns view catch-up vs dbUpdateSeq without building.
func (mgr *DefaultViewManager) GetViewStatus(designDocID, viewName string, dbUpdateSeq int64) (*ViewStatus, error) {
	status := &ViewStatus{DBUpdateSeq: dbUpdateSeq}

	qualifiedViewName := designDocID + "$" + viewName
	mgr.rwMutex.RLock()
	view, open := mgr.views[qualifiedViewName]
	var currentSeq int64
	if open {
		currentSeq = view.currentSeq
	}
	mgr.rwMutex.RUnlock()

	if open {
		status.Open = true
		status.Built = true
		status.ViewUpdateSeq = currentSeq
	} else {
		_, viewFileName := mgr.localDB.GetViewFileName(mgr.DBName, qualifiedViewName)
		if viewFileName == "" {
			status.Built = false
			status.ViewUpdateSeq = 0
		} else {
			viewFilePath := filepath.Join(mgr.viewDirPath, viewFileName+dbExt)
			seq, built, err := readViewMetaNextSeq(viewFilePath)
			if err != nil {
				return nil, err
			}
			status.Built = built
			status.ViewUpdateSeq = seq
		}
	}

	lag := status.DBUpdateSeq - status.ViewUpdateSeq
	if lag < 0 {
		lag = 0
	}
	status.Lag = lag
	return status, nil
}

func readViewMetaNextSeq(viewFilePath string) (int64, bool, error) {
	if _, err := os.Stat(viewFilePath); err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	con, err := sqlite3.Open(sqliteFileURI(viewFilePath, "ro"))
	if err != nil {
		return 0, false, err
	}
	defer con.Close()

	stmt, err := con.Prepare("SELECT next_update_seq FROM view_meta WHERE Id = 1")
	if err != nil {
		return 0, true, nil
	}
	defer stmt.Close()

	hasRow, err := stmt.Step()
	if err != nil || !hasRow {
		return 0, true, nil
	}
	var seq int64
	if err := stmt.Scan(&seq); err != nil {
		return 0, true, nil
	}
	return seq, true, nil
}

func runViewAtelierDryRun(docsDBPath string, mgr *DefaultViewManager, req ViewAtelierDryRunRequest) (*ViewAtelierDryRunResult, error) {
	limit := req.Limit
	if limit <= 0 {
		limit = defaultAtelierLimit
	}
	since := req.Since
	if since < 0 {
		since = 0
	}

	result := &ViewAtelierDryRunResult{
		OK:     true,
		Since:  since,
		Errors: []ViewAtelierSQLError{},
	}

	if kwErr := checkViewSQLKeywords(req.Setup, req.Run, req.Select); kwErr != nil {
		result.OK = false
		result.Errors = append(result.Errors, *kwErr)
		result.Error = ErrInvalidSQLStmt.Error()
		result.Reason = sqlErrorReason(*kwErr)
		return result, fmt.Errorf("%s: %w", result.Reason, ErrInvalidSQLStmt)
	}

	con, err := sqlite3.Open(":memory:")
	if err != nil {
		return nil, err
	}
	defer con.Close()

	buildSQL := `
		CREATE TABLE IF NOT EXISTS view_meta (
			Id						INTEGER PRIMARY KEY,
			current_update_seq	  	INT,
			next_update_seq	  		INT
		) WITHOUT ROWID;

		INSERT INTO view_meta (Id, current_update_seq, next_update_seq)
			SELECT 1,0,0 WHERE NOT EXISTS (SELECT 1 FROM view_meta WHERE Id = 1);
	`
	if err := con.Exec(buildSQL); err != nil {
		return nil, err
	}
	if err := setupViewDatabase(con, docsDBPath); err != nil {
		return nil, err
	}

	for i, x := range req.Setup {
		if strings.TrimSpace(x) == "" {
			continue
		}
		if err := con.Exec(x); err != nil {
			se := ViewAtelierSQLError{Phase: "setup", Index: i, SQL: x, Message: err.Error()}
			result.OK = false
			result.Errors = append(result.Errors, se)
			result.Error = ErrInvalidSQLStmt.Error()
			result.Reason = sqlErrorReason(se)
			return result, fmt.Errorf("%s: %w", result.Reason, ErrInvalidSQLStmt)
		}
	}

	windowSQL := `
		UPDATE view_meta SET
			current_update_seq = ?,
			next_update_seq = (
				SELECT IFNULL(MAX(update_seq), ?) FROM (
					SELECT update_seq FROM documents
					WHERE update_seq > ?
					ORDER BY update_seq
					LIMIT ?
				)
			)
	`
	if err := con.Exec(windowSQL, since, since, since, limit); err != nil {
		return nil, err
	}

	var currentSeq, nextSeq int64
	metaStmt, err := con.Prepare("SELECT current_update_seq, next_update_seq FROM view_meta WHERE Id = 1")
	if err != nil {
		return nil, err
	}
	if _, err := metaStmt.Step(); err != nil {
		metaStmt.Close()
		return nil, err
	}
	if err := metaStmt.Scan(&currentSeq, &nextSeq); err != nil {
		metaStmt.Close()
		return nil, err
	}
	metaStmt.Close()
	result.CurrentSeq = currentSeq
	result.NextSeq = nextSeq

	countStmt, err := con.Prepare("SELECT COUNT(*) FROM latest_changes")
	if err != nil {
		return nil, err
	}
	if _, err := countStmt.Step(); err != nil {
		countStmt.Close()
		return nil, err
	}
	if err := countStmt.Scan(&result.DocsInWindow); err != nil {
		countStmt.Close()
		return nil, err
	}
	countStmt.Close()

	for i, x := range req.Run {
		if strings.TrimSpace(x) == "" {
			continue
		}
		if err := con.Exec(x); err != nil {
			se := ViewAtelierSQLError{Phase: "run", Index: i, SQL: x, Message: err.Error()}
			result.OK = false
			result.Errors = append(result.Errors, se)
			result.Error = ErrInvalidSQLStmt.Error()
			result.Reason = sqlErrorReason(se)
			return result, fmt.Errorf("%s: %w", result.Reason, ErrInvalidSQLStmt)
		}
	}

	tables, err := atelierTableCounts(con)
	if err != nil {
		return nil, err
	}
	result.Tables = tables

	selectName := req.SelectName
	if selectName == "" {
		selectName = "default"
	}
	if req.Select != nil {
		if sel, ok := req.Select[selectName]; ok && strings.TrimSpace(sel) != "" {
			text, params := mgr.ParseQueryParams(sel)
			values := url.Values{}
			for k, v := range req.Params {
				values.Set(k, v)
			}
			rs, selErr := atelierSelect(con, text, params, values)
			if selErr != nil {
				se := ViewAtelierSQLError{Phase: "select", Index: 0, SQL: sel, Message: selErr.Error()}
				result.OK = false
				result.Errors = append(result.Errors, se)
				result.Error = ErrInvalidSQLStmt.Error()
				result.Reason = sqlErrorReason(se)
				return result, fmt.Errorf("%s: %w", result.Reason, ErrInvalidSQLStmt)
			}
			if len(rs) > 0 {
				if json.Valid(rs) {
					result.Result = json.RawMessage(rs)
				} else {
					wrapped, _ := json.Marshal(string(rs))
					result.Result = json.RawMessage(wrapped)
				}
			}
		}
	}

	if req.IncludeSQL {
		setupQ := make([]Query, 0, len(req.Setup))
		for _, x := range req.Setup {
			setupQ = append(setupQ, Query{text: x})
		}
		runQ := make([]Query, 0, len(req.Run))
		for _, x := range req.Run {
			runQ = append(runQ, Query{text: x})
		}
		vs := NewViewSQL(mgr.DBName, docsDBPath, "", setupQ, runQ)
		if err := vs.Open(); err != nil {
			return nil, err
		}
		sqlBytes, err := vs.SQL(since)
		if err != nil {
			return nil, err
		}
		result.GeneratedSQL = string(sqlBytes)
	}

	return result, nil
}

func atelierTableCounts(con *sqlite3.Conn) ([]ViewAtelierTableCount, error) {
	stmt, err := con.Prepare(`
		SELECT tbl_name FROM sqlite_master
		WHERE type = 'table' AND tbl_name NOT LIKE 'sqlite_%' AND tbl_name != 'view_meta'
		ORDER BY tbl_name
	`)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var tables []ViewAtelierTableCount
	hasRow, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	for hasRow {
		var name string
		if err := stmt.Scan(&name); err != nil {
			return nil, err
		}
		var rows int64
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdent(name))
		cstmt, err := con.Prepare(countSQL)
		if err != nil {
			return nil, err
		}
		if _, err := cstmt.Step(); err != nil {
			cstmt.Close()
			return nil, err
		}
		if err := cstmt.Scan(&rows); err != nil {
			cstmt.Close()
			return nil, err
		}
		cstmt.Close()
		tables = append(tables, ViewAtelierTableCount{Name: name, Rows: rows})
		hasRow, err = stmt.Step()
		if err != nil {
			return nil, err
		}
	}
	return tables, nil
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func atelierSelect(con *sqlite3.Conn, text string, params []string, values url.Values) ([]byte, error) {
	stmt, err := con.Prepare(text)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	pValues := make([]interface{}, len(params))
	for i, p := range params {
		pv := values.Get(p)
		if pv != "" {
			pValues[i] = pv
		}
	}
	if len(pValues) > 0 {
		if err := stmt.Bind(pValues...); err != nil {
			return nil, err
		}
	}

	hasRow, err := stmt.Step()
	if err != nil {
		return nil, err
	}
	if !hasRow {
		return nil, nil
	}

	var rs string
	if err := stmt.Scan(&rs); err != nil {
		o := viewResultValidation.FindAllStringSubmatch(err.Error(), -1)
		if len(o) > 0 {
			return nil, fmt.Errorf("%s: %w", fmt.Sprintf("select have %s, want 1 column", o[0][1]), ErrViewResult)
		}
		return nil, err
	}
	return []byte(rs), nil
}
