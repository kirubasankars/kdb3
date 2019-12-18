package main

import (
	"net/url"
	"database/sql"
	"fmt"
)

type ViewReader interface {
	Open() error
	Close() error
	Select(name string, values url.Values) ([]byte, error)
}

type DefaultViewReader struct {
	connectionString string
	selectScripts    map[string]Query
	setupDatabase    func(db *sql.DB) error

	con *sql.DB
}

func (vr *DefaultViewReader) Open() error {
	db, err := sql.Open("sqlite3", vr.connectionString)
	if err != nil {
		return err
	}
	vr.con = db

	return vr.setupDatabase(db)
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

func NewViewReader(connectionString string, selectScripts map[string]Query) (*DefaultViewReader, error) {
	viewReader := new(DefaultViewReader)
	viewReader.connectionString = connectionString
	viewReader.selectScripts = selectScripts
	return viewReader, nil
}

