package main

import (
	"os"
)

// FileHandler file system handler
type FileHandler interface {
	IsFileExists(path string) bool
	MkdirAll(path string) error
}

// DefaultFileHandler default implmentation of FileHandler
type DefaultFileHandler struct {
}

// IsFileExists check the existance of file or directory
func (fh *DefaultFileHandler) IsFileExists(path string) bool {
	_, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

// MkdirAll create directory
func (fh *DefaultFileHandler) MkdirAll(path string) error {
	return os.MkdirAll(path, 0755)
}
