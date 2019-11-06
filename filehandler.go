package main

import (
	"os"
)

type FileHandler interface {
	IsFileExists(path string) bool
	MkdirAll(path string) error
}

type DefaultFileHandler struct {
}

func (fh *DefaultFileHandler) IsFileExists(path string) bool {
	_, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false
	}
	return true
}

func (fh *DefaultFileHandler) MkdirAll(path string) error {
	return os.MkdirAll(path, 0755)
}
