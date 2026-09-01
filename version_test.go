package main

import (
	"encoding/json"
	"testing"
)

func TestVersionIdentityDefaults(t *testing.T) {
	if Version == "" {
		t.Fatal("Version must be non-empty")
	}
	if GitHash == "" {
		t.Fatal("GitHash must be non-empty")
	}
}

func TestInfoIncludesBuildAndSQLite(t *testing.T) {
	kdb, err := NewKDB()
	if err != nil {
		t.Fatal(err)
	}
	var info struct {
		Name    string `json:"name"`
		Version struct {
			KDB3   string `json:"kdb3"`
			Commit string `json:"commit"`
			SQLite string `json:"sqlite"`
		} `json:"version"`
	}
	if err := json.Unmarshal(kdb.Info(), &info); err != nil {
		t.Fatal(err)
	}
	if info.Name != "kdb3" {
		t.Fatalf("name: got %q", info.Name)
	}
	if info.Version.KDB3 != Version {
		t.Fatalf("version.kdb3: got %q want %q", info.Version.KDB3, Version)
	}
	if info.Version.Commit != GitHash {
		t.Fatalf("version.commit: got %q want %q", info.Version.Commit, GitHash)
	}
	if info.Version.SQLite == "" {
		t.Fatal("version.sqlite must be set")
	}
}
