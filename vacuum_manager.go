package main

import (
	"database/sql"
	"path/filepath"
)

type VacuumManager interface {
	SetNewConnectionString(connectionString string)
	SetCurrentConnectionString(currentDatabasePath, connectionString string)
	SetupDatabase() error
	CopyData(minUpdateSequence string, maxUpdateSequence string)
}

type DefaultVacuumManager struct {
	currentDatabasePath 			string
	currentConnectionString 		string

	newConnectionString 			string
}

func (vm *DefaultVacuumManager) SetNewConnectionString(connectionString string) {
	vm.newConnectionString = connectionString
}

func (vm *DefaultVacuumManager) SetCurrentConnectionString(currentDatabasePath, connectionString string) {
	vm.currentDatabasePath = currentDatabasePath
	vm.currentConnectionString = connectionString
}

func (vm DefaultVacuumManager) SetupDatabase() error {
	con, err := sql.Open("sqlite3", vm.newConnectionString)
	if err != nil {
		return err
	}
	err = con.Ping()
	if err != nil {
		return err
	}
	buildSQL := SetupDatabaseScript()
	tx, err := con.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(buildSQL)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	err = con.Close()
	if err != nil {
		return err
	}

	return nil
}

func (vm DefaultVacuumManager) CopyData(minUpdateSequence string, maxUpdateSequence string) {
	absoluteCurrentDatabasePath, _ := filepath.Abs(vm.currentDatabasePath)
	con, _ := sql.Open("sqlite3", vm.newConnectionString)
	con.Ping()
	con.Exec("ATTACH DATABASE 'file://" + absoluteCurrentDatabasePath + "?_journal=WAL&_locking_mode=EXCLUSIVE&cache=shared&_mutex=no&mode=ro' as currentdb;")
	tx, _ := con.Begin()
	if minUpdateSequence == "" {
		tx.Exec("INSERT INTO documents SELECT * FROM currentdb.documents WHERE seq_id <= ?", maxUpdateSequence)
	} else {
		tx.Exec("INSERT INTO documents SELECT * FROM currentdb.documents WHERE seq_id > ? AND seq_id <= ?", minUpdateSequence, maxUpdateSequence)
	}
	tx.Commit()
	con.Close()
}