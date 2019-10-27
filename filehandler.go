package main

import (
	"os"
)

type FileHandler interface {
	IsFileExists(path string) bool
}

type DefaultFileHandler struct {
}

func (fh *DefaultFileHandler) IsFileExists(path string) bool {
	_, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false
	} else {
		return true
	}
	return false
}
