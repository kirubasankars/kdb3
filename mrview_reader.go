package main

import (
	"database/sql"
	"fmt"
	"net/url"
)

type ViewReader interface {
	Open() error
	Close() error
	Select(name string, values url.Values) ([]byte, error)
}

type ViewReaderPool interface {
	Open() error
	Borrow() ViewReader
	Return(r ViewReader)
	Close() error
}

type DefaultViewReaderPool struct {
	connectionString     string
	absoluteDatabasePath string
	selectScripts        map[string]Query

	serviceLocator ServiceLocator
	pool           chan ViewReader
	limit          int
}

func (p *DefaultViewReaderPool) Open() error {
	for x := 0; x < p.limit; x++ {
		r := p.serviceLocator.GetViewReader(p.connectionString, p.absoluteDatabasePath, p.selectScripts)
		err := r.Open()
		if err != nil {
			panic(err)
		}
		p.pool <- r
	}
	return nil
}

func (p *DefaultViewReaderPool) Borrow() ViewReader {
	return <-p.pool
}

func (p *DefaultViewReaderPool) Return(r ViewReader) {
	p.pool <- r
}

func (p *DefaultViewReaderPool) Close() error {
	var err error

	count := 0
	for {
		var r ViewReader
		select {
		case r = <-p.pool:
			err = r.Close()
			count++
		default:
		}
		if count == p.limit {
			break
		}
	}

	return err
}

func NewViewReaderPool(connectionString, absoluteDatabasePath string, limit int, serviceLocator ServiceLocator, selectScripts map[string]Query) ViewReaderPool {
	readers := DefaultViewReaderPool{
		connectionString:     connectionString,
		absoluteDatabasePath: absoluteDatabasePath,
		pool:                 make(chan ViewReader, limit),
		limit:                limit,
		selectScripts:        selectScripts,
		serviceLocator:       serviceLocator,
	}
	return &readers
}

type DefaultViewReader struct {
	connectionString     string
	absoluteDatabasePath string
	selectScripts        map[string]Query

	con *sql.DB
}

func (vr *DefaultViewReader) Open() error {
	db, err := sql.Open("sqlite3", vr.connectionString)
	if err != nil {
		return err
	}
	vr.con = db

	return setupDatabase(db, vr.absoluteDatabasePath)
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

func NewViewReader(connectionString, absoluteDatabasePath string, selectScripts map[string]Query) *DefaultViewReader {
	viewReader := new(DefaultViewReader)
	viewReader.connectionString = connectionString
	viewReader.absoluteDatabasePath = absoluteDatabasePath
	viewReader.selectScripts = selectScripts
	return viewReader
}
