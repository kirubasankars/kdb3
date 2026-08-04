package main

import (
	"path/filepath"

	"kdb3/sqlite3"
)

type ViewWriter interface {
	Open() error
	Close() error
	Build(nextSeq int64) error
}

type DefaultViewWriter struct {
	connectionString string
	dbName           string
	con              *sqlite3.Conn

	absoluteDatabasePath string
	setupScripts         []Query
	scripts              []Query

	stmtUpdateViewMeta *sqlite3.Stmt
	stmtRunScripts     []*sqlite3.Stmt
}

func (vw *DefaultViewWriter) Open() error {
	db, err := sqlite3.Open(vw.connectionString)
	if err != nil {
		return err
	}
	vw.con = db

	err = db.Exec("PRAGMA journal_mode=MEMORY;")
	if err != nil {
		return err
	}
	_ = db.Exec("PRAGMA busy_timeout=5000;")

	buildSQL := `
		CREATE TABLE IF NOT EXISTS view_meta (
			Id						INTEGER PRIMARY KEY,
			current_update_seq		INT,
			next_update_seq		  	INT
		) WITHOUT ROWID;

		INSERT INTO view_meta (Id, current_update_seq, next_update_seq)
			SELECT 1,0,0 WHERE NOT EXISTS (SELECT 1 FROM view_meta WHERE Id = 1);
	`

	err = db.WithTx(func() error {
		if err := db.Exec(buildSQL); err != nil {
			return err
		}
		if err = setupViewDatabase(db, vw.absoluteDatabasePath); err != nil {
			return err
		}
		for _, x := range vw.setupScripts {
			if err = db.Exec(x.text); err != nil {
				return err
			}
		}

		vw.stmtUpdateViewMeta, err = db.Prepare("UPDATE view_meta SET current_update_seq = next_update_seq, next_update_seq = ?")
		if err != nil {
			return err
		}

		vw.stmtRunScripts = make([]*sqlite3.Stmt, 0, len(vw.scripts))
		for _, x := range vw.scripts {
			stmt, err := db.Prepare(x.text)
			if err != nil {
				return err
			}
			vw.stmtRunScripts = append(vw.stmtRunScripts, stmt)
		}
		return nil
	})

	return err
}

func (vw *DefaultViewWriter) Close() error {
	if vw.stmtUpdateViewMeta != nil {
		_ = vw.stmtUpdateViewMeta.Close()
	}
	for _, stmt := range vw.stmtRunScripts {
		if stmt != nil {
			_ = stmt.Close()
		}
	}
	vw.stmtRunScripts = nil
	return vw.con.Close()
}

func (vw *DefaultViewWriter) Build(nextSeq int64) error {
	db := vw.con

	err := db.WithTx(func() error {
		defer vw.stmtUpdateViewMeta.Reset()
		if err := vw.stmtUpdateViewMeta.Exec(nextSeq); err != nil {
			return err
		}
		for _, stmt := range vw.stmtRunScripts {
			if err := stmt.Exec(); err != nil {
				_ = stmt.Reset()
				return err
			}
			_ = stmt.Reset()
		}
		return nil
	})

	return err
}

func NewViewWriter(DBName, DBPath, connectionString string, setupScripts, scripts []Query) *DefaultViewWriter {
	viewWriter := new(DefaultViewWriter)
	viewWriter.connectionString = connectionString
	viewWriter.dbName = DBName
	viewWriter.setupScripts = setupScripts
	viewWriter.scripts = scripts

	absoluteDatabasePath, err := filepath.Abs(DBPath)
	if err != nil {
		absoluteDatabasePath = DBPath
	}
	viewWriter.absoluteDatabasePath = absoluteDatabasePath

	return viewWriter
}
