package main

import (
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
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

	con *sqlite3.Conn
}

func (vr *DefaultViewReader) Open() error {
	var err error
	vr.con, err = sqlite3.Open(vr.connectionString)
	if err != nil {
		return err
	}

	//vr.con.Exec("PRAGMA journal_mode=WAL;")

	db := vr.con
	err = db.WithTx(func() error {
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

	stmt, err := vr.con.Prepare(selectStmt.text, pValues...)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

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

	absoluteDBPath, err := filepath.Abs(DBPath)
	if err != nil {
		panic(err)
	}
	viewReader.absoluteDatabasePath = absoluteDBPath

	return viewReader
}
