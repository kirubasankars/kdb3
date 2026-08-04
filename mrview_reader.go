package main

import (
	"fmt"
	"net/url"
	"path/filepath"

	"kdb3/sqlite3"
)

type ViewReader interface {
	Open() error
	Close() error
	Select(name string, values url.Values) ([]byte, error)
}

type DefaultViewReader struct {
	connectionString     string
	absoluteDatabasePath string
	selectScripts        map[string]Query
	setupScripts         []Query
	dbName               string
	con                  *sqlite3.Conn
	preparedSelects      map[string]*sqlite3.Stmt
}

func (vr *DefaultViewReader) Open() error {
	var err error
	if vr.con, err = sqlite3.Open(vr.connectionString); err != nil {
		return err
	}
	db := vr.con

	if err = db.Exec("PRAGMA journal_mode=MEMORY;"); err != nil {
		return err
	}
	_ = db.Exec("PRAGMA busy_timeout=5000;")

	err = db.WithTx(func() error {
		if err = setupViewDatabase(vr.con, vr.absoluteDatabasePath); err != nil {
			return err
		}

		for _, x := range vr.setupScripts {
			if err = db.Exec(x.text); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return err
	}

	vr.preparedSelects = make(map[string]*sqlite3.Stmt, len(vr.selectScripts))
	for name, q := range vr.selectScripts {
		stmt, err := db.Prepare(q.text)
		if err != nil {
			return err
		}
		vr.preparedSelects[name] = stmt
	}
	return nil
}

func (vr *DefaultViewReader) Close() error {
	for _, stmt := range vr.preparedSelects {
		if stmt != nil {
			_ = stmt.Close()
		}
	}
	vr.preparedSelects = nil
	return vr.con.Close()
}

func (vr *DefaultViewReader) Select(name string, values url.Values) ([]byte, error) {
	var rs string
	selectStmt, ok := vr.selectScripts[name]
	if !ok || selectStmt.text == "" {
		return nil, fmt.Errorf("%s: %w", name, ErrViewNotFound)
	}
	stmt := vr.preparedSelects[name]
	if stmt == nil {
		return nil, fmt.Errorf("%s: %w", name, ErrViewNotFound)
	}

	pValues := make([]interface{}, len(selectStmt.params))
	for i, p := range selectStmt.params {
		pv := values.Get(p)
		if pv != "" {
			pValues[i] = pv
		}
	}

	defer stmt.Reset()
	if len(pValues) > 0 {
		if err := stmt.Bind(pValues...); err != nil {
			return nil, err
		}
	}

	hasRow, err := stmt.Step()
	if err != nil {
		return nil, err
	}

	if hasRow {
		err := stmt.Scan(&rs)
		if err != nil {
			o := viewResultValidation.FindAllStringSubmatch(err.Error(), -1)
			if len(o) > 0 {
				return nil, fmt.Errorf("%s: %w", fmt.Sprintf("select have %s, want 1 column", o[0][1]), ErrViewResult)
			}
			return nil, err
		}
		return []byte(rs), nil
	}
	return nil, nil
}

func NewViewReader(DBName string, DBPath string, connectionString string, scripts []Query, selectScripts map[string]Query) *DefaultViewReader {
	viewReader := new(DefaultViewReader)
	viewReader.connectionString = connectionString
	viewReader.selectScripts = selectScripts
	viewReader.setupScripts = scripts
	viewReader.dbName = DBName

	absoluteDBPath, err := filepath.Abs(DBPath)
	if err != nil {
		absoluteDBPath = DBPath
	}
	viewReader.absoluteDatabasePath = absoluteDBPath

	return viewReader
}
