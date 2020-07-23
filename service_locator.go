package main

import "path/filepath"

// ServiceLocator interface
type ServiceLocator interface {
	GetFileHandler() FileHandler
	GetLocalDB() LocalDB

	GetDBDirPath() string
	GetViewDirPath() string

	GetDatabase(dbName string, createIfNotExists bool) Database
	GetDatabaseWriter(dbName string) DatabaseWriter
	GetDatabaseReader(dbName string) DatabaseReader

	GetViewManager(dbName string) ViewManager
	GetViewReader(dbName, docID, viewName string, selectScripts map[string]Query) ViewReader
	GetViewWriter(dbName, docID, viewName string, setup, scripts []Query) ViewWriter
}

// DefaultServiceLocator default implementation of ServiceLocator
type DefaultServiceLocator struct {
	fileHandler *DefaultFileHandler
	localDB     LocalDB

	dbDirPath   string
	viewDirPath string
}

// GetFileHandler resolve FileHandler instance
func (serviceLocator *DefaultServiceLocator) GetFileHandler() FileHandler {
	return serviceLocator.fileHandler
}

func (serviceLocator *DefaultServiceLocator) GetDBDirPath() string {
	return serviceLocator.dbDirPath
}

func (serviceLocator *DefaultServiceLocator) GetViewDirPath() string {
	return serviceLocator.viewDirPath
}

// GetDatabaseWriter resolve DatabaseWriter instance
func (serviceLocator *DefaultServiceLocator) GetDatabaseWriter(dbName string) DatabaseWriter {
	fileName := serviceLocator.localDB.GetDatabaseFileName(dbName)
	connectionString := filepath.Join(serviceLocator.dbDirPath, fileName+dbExt) + "?_journal=WAL&cache=shared&_mutex=no&mode=rwc"
	databaseWriter := new(DefaultDatabaseWriter)
	databaseWriter.reader = new(DefaultDatabaseReader)
	databaseWriter.connectionString = connectionString
	return databaseWriter
}

// GetDatabaseReader resolve DatabaseReader instance
func (serviceLocator *DefaultServiceLocator) GetDatabaseReader(dbName string) DatabaseReader {
	fileName := serviceLocator.localDB.GetDatabaseFileName(dbName)
	connectionString := filepath.Join(serviceLocator.GetDBDirPath(), fileName+dbExt) + "?_journal=WAL&cache=shared&_mutex=no&mode=ro"
	databaseReader := new(DefaultDatabaseReader)
	databaseReader.connectionString = connectionString
	return databaseReader
}

// GetViewManager resolve ViewManager instance
func (serviceLocator *DefaultServiceLocator) GetViewManager(dbName string) ViewManager {
	fileName := serviceLocator.localDB.GetDatabaseFileName(dbName)
	DBPath := filepath.Join(serviceLocator.GetDBDirPath(), fileName+dbExt)
	return NewViewManager(dbName, DBPath, serviceLocator.viewDirPath, serviceLocator)
}

// GetViewReader resolve ViewReader instance
func (serviceLocator *DefaultServiceLocator) GetViewReader(dbName, docID, viewName string, selectScripts map[string]Query) ViewReader {
	fileName := serviceLocator.localDB.GetDatabaseFileName(dbName)
	DBPath := filepath.Join(serviceLocator.GetDBDirPath(), fileName+dbExt)

	qualifiedViewName := docID + "$" + viewName
	_, viewFileName := serviceLocator.localDB.GetViewFileName(dbName, qualifiedViewName)
	viewFilePath := filepath.Join(serviceLocator.GetViewDirPath(), viewFileName+dbExt)
	connectionString := viewFilePath + "?_journal=MEMORY&cache=shared&_mutex=no&mode=ro"
	return NewViewReader(dbName, DBPath, connectionString, selectScripts)
}

// GetViewWriter resolve ViewWriter instance
func (serviceLocator *DefaultServiceLocator) GetViewWriter(dbName, docID, viewName string, setup, scripts []Query) ViewWriter {
	fileName := serviceLocator.localDB.GetDatabaseFileName(dbName)
	DBPath := filepath.Join(serviceLocator.GetDBDirPath(), fileName+dbExt)

	qualifiedViewName := docID + "$" + viewName
	_, viewFileName := serviceLocator.localDB.GetViewFileName(dbName, qualifiedViewName)
	viewFilePath := filepath.Join(serviceLocator.GetViewDirPath(), viewFileName+dbExt)
	connectionString := viewFilePath + "?_journal=MEMORY&cache=shared&_mutex=no&mode=rwc"
	return NewViewWriter(dbName, DBPath, connectionString, setup, scripts)
}

// GetLocalDB resolve LocalDB instance
func (serviceLocator *DefaultServiceLocator) GetLocalDB() LocalDB {
	return serviceLocator.localDB
}

// GetDatabase resolve database instance
func (serviceLocator *DefaultServiceLocator) GetDatabase(dbName string, createIfNotExists bool) Database {
	return NewDatabase(dbName, createIfNotExists, serviceLocator)
}

// NewServiceLocator create new ServiceLocator
func NewServiceLocator() ServiceLocator {
	serviceLocator := new(DefaultServiceLocator)
	serviceLocator.dbDirPath = "./data/dbs"
	serviceLocator.viewDirPath = "./data/views"
	serviceLocator.fileHandler = new(DefaultFileHandler)
	serviceLocator.localDB = NewLocalDB()
	return serviceLocator
}
