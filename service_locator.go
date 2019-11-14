package main

type ServiceLocator interface {
	GetFileHandler() FileHandler

	GetDatabaseWriter(connectionString string) DatabaseWriter

	GetDatabaseReader(connectionString string) DatabaseReader
	GetDatabaseReaderPool(connectionString string, limit int) DatabaseReaderPool

	GetViewManager(dbName, absoluteDatabasePath, viewPath string) ViewManager
	GetView(viewName, connectionString, absoluteDatabasePath string, ddoc *DesignDocument, viewManager ViewManager) *View
}

type DefaultServiceLocator struct {
	fileHandler *DefaultFileHandler
}

func (sl *DefaultServiceLocator) GetFileHandler() FileHandler {
	return sl.fileHandler
}

func (sl *DefaultServiceLocator) GetDatabaseWriter(connectionString string) DatabaseWriter {
	databaseWriter := new(DefaultDatabaseWriter)
	databaseWriter.connectionString = connectionString
	databaseWriter.reader = new(DefaultDatabaseReader)
	return databaseWriter
}

func (sl *DefaultServiceLocator) GetDatabaseReaderPool(connectionString string, limit int) DatabaseReaderPool {
	databaseReaders := NewDatabaseReaderPool(connectionString, limit, sl)
	return databaseReaders
}

func (sl *DefaultServiceLocator) GetDatabaseReader(connectionString string) DatabaseReader {
	databaseReader := new(DefaultDatabaseReader)
	databaseReader.connectionString = connectionString
	return databaseReader
}

func (sl *DefaultServiceLocator) GetViewManager(dbName, absoluteDatabasePath, viewPath string) ViewManager {
	return NewViewManager(dbName, absoluteDatabasePath, viewPath, sl)
}

func (sl *DefaultServiceLocator) GetView(viewName, connectionString, absoluteDatabasePath string, ddoc *DesignDocument, viewManager ViewManager) *View {
	return NewView(viewName, connectionString, absoluteDatabasePath, ddoc, viewManager)
}

func NewServiceLocator() ServiceLocator {
	serviceLocator := new(DefaultServiceLocator)
	serviceLocator.fileHandler = new(DefaultFileHandler)
	return serviceLocator
}
