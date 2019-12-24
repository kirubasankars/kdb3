package main

type ServiceLocator interface {
	GetFileHandler() FileHandler

	GetDatabaseWriter() DatabaseWriter
	GetDatabaseReader() DatabaseReader

	GetViewManager() ViewManager
	GetView(viewName, connectionString, absoluteDatabasePath string, ddoc *DesignDocument, viewManager ViewManager) *View

	GetViewReader(connectionString, absoluteDatabasePath string, selectScripts map[string]Query) ViewReader
}

type DefaultServiceLocator struct {
	fileHandler *DefaultFileHandler
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

func (sl *DefaultServiceLocator) GetView(viewName, connectionString, absoluteDatabasePath string, ddoc *DesignDocument, viewManager ViewManager) *View {
	return NewView(viewName, connectionString, absoluteDatabasePath, ddoc, viewManager, sl)
}

func (sl *DefaultServiceLocator) GetViewReader(connectionString, absoluteDatabasePath string, selectScripts map[string]Query) ViewReader {
	return NewViewReader(connectionString, absoluteDatabasePath, selectScripts)
}

func NewServiceLocator() ServiceLocator {
	serviceLocator := new(DefaultServiceLocator)
	serviceLocator.fileHandler = new(DefaultFileHandler)
	return serviceLocator
}
