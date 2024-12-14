package main

import (
	"path/filepath"

	"github.com/bvinc/go-sqlite-lite/sqlite3"
)

type VacuumManager interface {
	SetNewConnectionString(connectionString string)
	SetCurrentConnectionString(currentDatabasePath, connectionString string)
	SetupDatabase() error
	CopyData(minUpdateSequence int64, maxUpdateSequence int64) error
	Vacuum() error
}

type DefaultVacuumManager struct {
	currentDatabasePath     string
	currentConnectionString string

	newConnectionString string
}

func (vm *DefaultVacuumManager) SetNewConnectionString(connectionString string) {
	absoluteNewDatabasePath, _ := filepath.Abs(connectionString)
	vm.newConnectionString = absoluteNewDatabasePath
}

func (vm *DefaultVacuumManager) SetCurrentConnectionString(currentDatabasePath, connectionString string) {
	vm.currentDatabasePath = currentDatabasePath
	absoluteCurrentDatabasePath, _ := filepath.Abs(currentDatabasePath)
	vm.currentConnectionString = absoluteCurrentDatabasePath
}

func (vm DefaultVacuumManager) SetupDatabase() error {
	absoluteNewDatabasePath, _ := filepath.Abs(vm.newConnectionString)
	con, err := sqlite3.Open("file:" + absoluteNewDatabasePath + "?_locking_mode=EXCLUSIVE&_mutex=no&mode=rwc")
	if err != nil {
		return err
	}
	buildSQL := SetupDatabaseScript()
	err = con.Begin()
	if err != nil {
		return err
	}

	err = con.Exec(buildSQL)
	if err != nil {
		return err
	}

	err = con.Commit()
	if err != nil {
		return err
	}

	err = con.Close()
	if err != nil {
		return err
	}

	return nil
}

func (vm DefaultVacuumManager) CopyData(minUpdateSequence int64, maxUpdateSequence int64) error {
	absoluteCurrentDatabasePath, _ := filepath.Abs(vm.currentDatabasePath)

	con, err := sqlite3.Open("file:" + vm.newConnectionString + "?_locking_mode=EXCLUSIVE&_mutex=no&mode=rwc")
	if err != nil {
		return err
	}
	defer con.Close()

	con.Exec("ATTACH DATABASE 'file:" + absoluteCurrentDatabasePath + "' as currentdb;")

	err = con.Begin()
	if err != nil {
		return err
	}
	defer func() {
		con.Rollback()
		con.Close()
	}()

	if minUpdateSequence == 0 {
		err = con.Exec("INSERT INTO documents SELECT * FROM currentdb.documents WHERE update_seq <= ?", maxUpdateSequence)
		if err != nil {
			return err
		}
		con.Commit()
	} else {
		err = con.Exec("INSERT INTO documents SELECT * FROM currentdb.documents WHERE update_seq > ? AND update_seq <= ?", minUpdateSequence, maxUpdateSequence)
		if err != nil {
			return err
		}
		con.Commit()
	}
	return nil
}

func (vm DefaultVacuumManager) Vacuum() error {
	con, err := sqlite3.Open("file:" + vm.newConnectionString)
	if err != nil {
		return err
	}
	defer con.Close()
	return con.Exec("VACUUM")
}
