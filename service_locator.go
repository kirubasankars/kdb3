package main

type ServiceLocator interface {
	GetFileHandler() FileHandler

	GetDatabaseWriter() DatabaseWriter
	GetDatabaseReader() DatabaseReader
	GetLocalDB() LocalDB

	GetViewManager() ViewManager

	GetView(viewName, viewFileName, viewFilePath, connectionString, absoluteDatabasePath string, ddoc *DesignDocument, viewManager ViewManager) *View
	GetViewReader(connectionString, absoluteDatabasePath string, selectScripts map[string]Query) ViewReader
}

type DefaultServiceLocator struct {
	fileHandler *DefaultFileHandler
	localdb     LocalDB
}

func (sl *DefaultServiceLocator) GetFileHandler() FileHandler {
	return sl.fileHandler
}

func (sl *DefaultServiceLocator) GetDatabaseWriter() DatabaseWriter {
	databaseWriter := new(DefaultDatabaseWriter)
	databaseWriter.reader = new(DefaultDatabaseReader)
	return databaseWriter
}

func (sl *DefaultServiceLocator) GetDatabaseReader() DatabaseReader {
	databaseReader := new(DefaultDatabaseReader)
	return databaseReader
}

func (sl *DefaultServiceLocator) GetViewManager() ViewManager {
	return NewViewManager(sl)
}

func (sl *DefaultServiceLocator) GetView(viewName, viewFileName, viewFilePath, connectionString, absoluteDatabasePath string, ddoc *DesignDocument, viewManager ViewManager) *View {
	return NewView(viewName, viewFileName, viewFilePath, connectionString, absoluteDatabasePath, ddoc, viewManager, sl)
}

func (sl *DefaultServiceLocator) GetViewReader(connectionString, absoluteDatabasePath string, selectScripts map[string]Query) ViewReader {
	return NewViewReader(connectionString, absoluteDatabasePath, selectScripts)
}

func (sl *DefaultServiceLocator) GetLocalDB() LocalDB {
	return sl.localdb
}

func NewServiceLocator() ServiceLocator {
	serviceLocator := new(DefaultServiceLocator)
	serviceLocator.fileHandler = new(DefaultFileHandler)
	serviceLocator.localdb = &DefaultLocalDB{}
	return serviceLocator
}
