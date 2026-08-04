package main

import "errors"

var errFakeOpen = errors.New("fake open failed")
var errFakeClose = errors.New("fake close failed")
var errFakeVacuum = errors.New("fake vacuum failed")

type failingVacuumManager struct {
	failAt string // "setup", "copy", or "vacuum"
	newCS  string
}

func (f *failingVacuumManager) SetNewConnectionString(cs string) { f.newCS = cs }
func (f *failingVacuumManager) SetCurrentConnectionString(_, _ string) {
}
func (f *failingVacuumManager) SetupDatabase() error {
	if f.failAt == "setup" {
		return errFakeVacuum
	}
	return nil
}
func (f *failingVacuumManager) CopyData(_, _ int64) error {
	if f.failAt == "copy" {
		return errFakeVacuum
	}
	return nil
}
func (f *failingVacuumManager) Vacuum() error {
	if f.failAt == "vacuum" {
		return errFakeVacuum
	}
	return nil
}

type failOpenReader struct {
	DatabaseReader
	opened bool
}

func (r *failOpenReader) Open() error { return errFakeOpen }
func (r *failOpenReader) Close() error {
	if r.DatabaseReader != nil {
		return r.DatabaseReader.Close()
	}
	return nil
}

type failCloseWriter struct {
	DatabaseWriter
	failClose bool
}

func (w *failCloseWriter) Close() error {
	if w.failClose {
		return errFakeClose
	}
	return w.DatabaseWriter.Close()
}
