package main

import "path/filepath"

func absPath(p string) string {
	abs, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	return abs
}

func sqliteFileURI(path, mode string) string {
	return "file:" + absPath(path) + "?cache=shared&mode=" + mode
}

// ServiceLocator interface
type ServiceLocator interface {
	GetFileHandler() FileHandler
	GetLocalDB() LocalDB

	GetDBDirPath() string
	GetViewDirPath() string

	GetDatabase(dbName string, createIfNotExists bool) (Database, error)
	GetDatabaseWriter(dbName string) DatabaseWriter
	GetDatabaseReader(dbName string) DatabaseReader

	GetViewManager(dbName string) ViewManager
	GetViewReader(dbName, docID, viewName string, scripts []Query, selectScripts map[string]Query) ViewReader
	GetViewWriter(dbName, docID, viewName string, setup, scripts []Query) ViewWriter
	GetViewSQLBuilder(dbName, docID, viewName string, setup, scripts []Query) *ViewSQLChangeSet

	GetVacuumManager(dbName string) VacuumManager
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

func (serviceLocator *DefaultServiceLocator) GetVacuumManager(dbName string) VacuumManager {
	vacuumManager := new(DefaultVacuumManager)
	return vacuumManager
}

// GetDatabaseWriter resolve DatabaseWriter instance
func (serviceLocator *DefaultServiceLocator) GetDatabaseWriter(dbName string) DatabaseWriter {
	fileName := serviceLocator.localDB.GetDatabaseFileName(dbName)
	dbPath := filepath.Join(serviceLocator.dbDirPath, fileName+dbExt)
	databaseWriter := new(DefaultDatabaseWriter)
	databaseWriter.reader = new(DefaultDatabaseReader)
	databaseWriter.connectionString = sqliteFileURI(dbPath, "rwc")
	return databaseWriter
}

// GetDatabaseReader resolve DatabaseReader instance
func (serviceLocator *DefaultServiceLocator) GetDatabaseReader(dbName string) DatabaseReader {
	fileName := serviceLocator.localDB.GetDatabaseFileName(dbName)
	dbPath := filepath.Join(serviceLocator.GetDBDirPath(), fileName+dbExt)
	databaseReader := new(DefaultDatabaseReader)
	databaseReader.connectionString = sqliteFileURI(dbPath, "ro")
	return databaseReader
}

// GetViewManager resolve ViewManager instance
func (serviceLocator *DefaultServiceLocator) GetViewManager(dbName string) ViewManager {
	return NewViewManager(dbName, serviceLocator.viewDirPath, serviceLocator)
}

// GetViewReader resolve ViewReader instance
func (serviceLocator *DefaultServiceLocator) GetViewReader(dbName, docID, viewName string, scripts []Query, selectScripts map[string]Query) ViewReader {
	fileName := serviceLocator.localDB.GetDatabaseFileName(dbName)
	DBPath := absPath(filepath.Join(serviceLocator.GetDBDirPath(), fileName+dbExt))

	qualifiedViewName := docID + "$" + viewName
	_, viewFileName := serviceLocator.localDB.GetViewFileName(dbName, qualifiedViewName)
	viewFilePath := filepath.Join(serviceLocator.GetViewDirPath(), viewFileName+dbExt)
	connectionString := sqliteFileURI(viewFilePath, "rw")
	return NewViewReader(dbName, DBPath, connectionString, scripts, selectScripts)
}

// GetViewSQL resolve ViewSQLChangeSet instance
func (serviceLocator *DefaultServiceLocator) GetViewSQLBuilder(dbName, docID, viewName string, setup, scripts []Query) *ViewSQLChangeSet {
	fileName := serviceLocator.localDB.GetDatabaseFileName(dbName)
	DBPath := absPath(filepath.Join(serviceLocator.GetDBDirPath(), fileName+dbExt))
	qualifiedViewName := docID + "$" + viewName
	return NewViewSQL(dbName, DBPath, qualifiedViewName, setup, scripts)
}

// GetViewWriter resolve ViewWriter instance
func (serviceLocator *DefaultServiceLocator) GetViewWriter(dbName, docID, viewName string, setup, scripts []Query) ViewWriter {
	fileName := serviceLocator.localDB.GetDatabaseFileName(dbName)
	DBPath := absPath(filepath.Join(serviceLocator.GetDBDirPath(), fileName+dbExt))

	qualifiedViewName := docID + "$" + viewName
	_, viewFileName := serviceLocator.localDB.GetViewFileName(dbName, qualifiedViewName)
	viewFilePath := filepath.Join(serviceLocator.GetViewDirPath(), viewFileName+dbExt)
	connectionString := sqliteFileURI(viewFilePath, "rwc")
	return NewViewWriter(dbName, DBPath, connectionString, setup, scripts)
}

// GetLocalDB resolve LocalDB instance
func (serviceLocator *DefaultServiceLocator) GetLocalDB() LocalDB {
	return serviceLocator.localDB
}

// GetDatabase resolve database instance
func (serviceLocator *DefaultServiceLocator) GetDatabase(dbName string, createIfNotExists bool) (Database, error) {
	return NewDatabase(dbName, createIfNotExists, serviceLocator)
}

// NewServiceLocator create new ServiceLocator under dataDir (dbs + views subdirs).
func NewServiceLocator(dataDir string) ServiceLocator {
	if dataDir == "" {
		dataDir = "./data"
	}
	serviceLocator := new(DefaultServiceLocator)
	serviceLocator.dbDirPath = filepath.Join(dataDir, "dbs")
	serviceLocator.viewDirPath = filepath.Join(dataDir, "views")
	serviceLocator.fileHandler = new(DefaultFileHandler)
	serviceLocator.localDB = NewLocalDB()
	return serviceLocator
}
