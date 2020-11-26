package main

import (
	"bytes"
	"fmt"
	"github.com/bvinc/go-sqlite-lite/sqlite3"
	"path/filepath"
	"strings"
)

type ViewSQLChangeSet struct {
	absoluteDatabasePath string
	setupScripts		 []Query
	scripts              []Query
	con 				 *sqlite3.Conn
}

func (vs *ViewSQLChangeSet) Open() error {
	var err error
	var con *sqlite3.Conn
	if con, err = sqlite3.Open(":memory:"); err != nil {
		return err
	}

	buildSQL := `
		CREATE TABLE IF NOT EXISTS view_meta (
			Id						INTEGER PRIMARY KEY,
			current_seq_id		  	TEXT,
			next_seq_id		  		TEXT
		) WITHOUT ROWID;
	
		INSERT INTO view_meta (Id, current_seq_id, next_seq_id) 
			SELECT 1,"", "" WHERE NOT EXISTS (SELECT 1 FROM view_meta WHERE Id = 1);
	`

	err = con.WithTx(func() error {
		if err := con.Exec(buildSQL); err != nil {
			return err
		}
		if err = setupViewDatabase(con, vs.absoluteDatabasePath); err != nil {
			return err
		}
		for _, x := range vs.setupScripts {
			if err = con.Exec(x.text); err != nil {
				return err
			}
		}
		return nil
	})

	vs.con = con

	return err
}

func (vs *ViewSQLChangeSet) SQL(seqID string) ([]byte, error) {
	db := vs.con
	defer db.Close()

	db.WithTx(func() error {
		if err := db.Exec("UPDATE view_meta SET current_seq_id = ?, next_seq_id = (SELECT IFNULL(MAX(seq_id),?) FROM documents WHERE seq_id >= ? ORDER BY seq_id LIMIT 300)", seqID, seqID, seqID); err != nil {
			return err
		}
		for _, x := range vs.scripts {
			if err := db.Exec(x.text); err != nil {
				return err
			}
		}
		return nil
	})

	var tables []string
	stmtTables, _ := db.Prepare("SELECT tbl_name FROM sqlite_master where type = 'table'")
	defer stmtTables.Close()
	moreRows, _ := stmtTables.Step()
	for moreRows {
		var table string
		if err := stmtTables.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
		moreRows, _ = stmtTables.Step()
	}

	var sqls = make(map[string]string)
	for _, table := range tables {
		stmt, _ := db.Prepare("SELECT * FROM " + table + " WHERE 1 = 2")
		stmt.Step()
		columns := stmt.ColumnNames()

		var exps []string
		for _, column := range columns {
			exp := fmt.Sprintf("quote(%s)", column)
			exps = append(exps, exp)
		}

		var ccolumns = strings.Join(columns, ",")
		var cexps = strings.Join(exps, "|| ',' ||")
		valuesSQL := fmt.Sprintf("'(' || %s || ')'", cexps)
		selectSQL := fmt.Sprintf("SELECT 'INSERT OR REPLACE INTO %s (%s) VALUES ' || GROUP_CONCAT(%s, ',') || ';' as rows FROM %s", table, ccolumns, valuesSQL, table)
		if strings.LastIndexAny(selectSQL, ";") == len(selectSQL) {
			selectSQL += ";"
		}
		sqls[table] = selectSQL

		stmt.Close()
	}

	var outputSQL bytes.Buffer

	outputSQL.WriteString("BEGIN;\n")
	if seqID == "" {
		outputSQL.WriteString(`CREATE TABLE IF NOT EXISTS view_meta (Id INTEGER PRIMARY KEY, current_seq_id TEXT, next_seq_id TEXT) WITHOUT ROWID;`)
		outputSQL.WriteString("\n")

		for _, q := range vs.setupScripts {
			outputSQL.WriteString(q.text + ";")
			outputSQL.WriteString("\n")
		}
	}

	for _, sql := range sqls {

		stmt, _ := db.Prepare(sql)
		stmt.Step()
		var row string
		stmt.Scan(&row)

		outputSQL.WriteString(row)
		outputSQL.WriteString("\n")
		stmt.Close()
	}
	outputSQL.WriteString("END;")

	return outputSQL.Bytes(), nil
}

func NewViewSQL(dbName, DBPath, qualifiedViewName string, setup, scripts []Query) *ViewSQLChangeSet {
	vs := new(ViewSQLChangeSet)
	absoluteDatabasePath, err := filepath.Abs(DBPath)
	if err != nil {
		panic(err)
	}
	vs.absoluteDatabasePath = absoluteDatabasePath
	vs.setupScripts = setup
	vs.scripts = scripts
	return vs
}