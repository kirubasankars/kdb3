package main

// ServiceLocator interface
type ServiceLocator interface {
	GetFileHandler() FileHandler

	GetDatabaseWriter() DatabaseWriter
	GetDatabaseReader() DatabaseReader
	GetLocalDB() LocalDB

	GetViewManager() ViewManager

	GetView(viewName, viewFileName, viewFilePath, connectionString, absoluteDatabasePath string, ddoc *DesignDocument, viewManager ViewManager) *View
	GetViewReader(connectionString, absoluteDatabasePath string, selectScripts map[string]Query) ViewReader
}

// DefaultServiceLocator default implementation of ServiceLocator
type DefaultServiceLocator struct {
	fileHandler *DefaultFileHandler
	localdb     LocalDB
}

// GetFileHandler resolve FileHandler instance
func (serviceLocator *DefaultServiceLocator) GetFileHandler() FileHandler {
	return serviceLocator.fileHandler
}

// GetDatabaseWriter resolve DatabaseWriter instance
func (serviceLocator *DefaultServiceLocator) GetDatabaseWriter() DatabaseWriter {
	databaseWriter := new(DefaultDatabaseWriter)
	databaseWriter.reader = new(DefaultDatabaseReader)
	return databaseWriter
}

// GetDatabaseReader resolve DatabaseReader instance
func (serviceLocator *DefaultServiceLocator) GetDatabaseReader() DatabaseReader {
	databaseReader := new(DefaultDatabaseReader)
	return databaseReader
}

// GetViewManager resolve ViewManager instance
func (serviceLocator *DefaultServiceLocator) GetViewManager() ViewManager {
	return NewViewManager(serviceLocator)
}

// GetView resolve View instance
func (serviceLocator *DefaultServiceLocator) GetView(viewName, viewFileName, viewFilePath, connectionString, absoluteDatabasePath string, ddoc *DesignDocument, viewManager ViewManager) *View {
	return NewView(viewName, viewFileName, viewFilePath, connectionString, absoluteDatabasePath, ddoc, viewManager, serviceLocator)
}

// GetViewReader resolve ViewReader instance
func (serviceLocator *DefaultServiceLocator) GetViewReader(connectionString, absoluteDatabasePath string, selectScripts map[string]Query) ViewReader {
	return NewViewReader(connectionString, absoluteDatabasePath, selectScripts)
}

// GetLocalDB resolve LocalDB instance
func (serviceLocator *DefaultServiceLocator) GetLocalDB() LocalDB {
	return serviceLocator.localdb
}

// NewServiceLocator create new servicelocator
func NewServiceLocator() ServiceLocator {
	serviceLocator := new(DefaultServiceLocator)
	serviceLocator.fileHandler = new(DefaultFileHandler)
	serviceLocator.localdb = &DefaultLocalDB{}
	return serviceLocator
}
