package main

import (
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
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

	con *sql.DB
}

func (vr *DefaultViewReader) Open() error {
	var err error
	vr.con, err = sql.Open("sqlite3", vr.connectionString)
	if err != nil {
		return err
	}

	db := vr.con

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	for _, x := range vr.setupScripts {
		if _, err = tx.Exec(x.text); err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return setupViewDatabase(vr.con, vr.absoluteDatabasePath)
}

func (vr *DefaultViewReader) Close() error {
	return vr.con.Close()
}

func (vr *DefaultViewReader) Select(name string, values url.Values) ([]byte, error) {
	var rs string
	selectStmt := vr.selectScripts[name]
	pValues := make([]interface{}, len(selectStmt.params))
	for i, p := range selectStmt.params {
		pv := values.Get(p)
		if pv != "" {
			pValues[i] = values.Get(p)
		}
	}

	row := vr.con.QueryRow(selectStmt.text, pValues...)
	err := row.Scan(&rs)
	if err != nil {
		o := viewResultValidation.FindAllStringSubmatch(err.Error(), -1)
		if len(o) > 0 {
			return nil, fmt.Errorf("%s: %w", fmt.Sprintf("select have %s, want 1 column", o[0][1]), ErrViewResult)
		}
		return nil, err
	}
	return []byte(rs), nil
}

func NewViewReader(DBName string, DBPath string, connectionString string, scripts []Query, selectScripts map[string]Query) *DefaultViewReader {
	viewReader := new(DefaultViewReader)
	viewReader.connectionString = connectionString
	viewReader.selectScripts = selectScripts
	viewReader.setupScripts = scripts

	absoluteDBPath, err := filepath.Abs(DBPath)
	if err != nil {
		panic(err)
	}
	viewReader.absoluteDatabasePath = absoluteDBPath

	return viewReader
}
