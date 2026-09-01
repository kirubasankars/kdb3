package main

// Version and GitHash are replaced at link time via -ldflags.
// Plain `go build` / `go test` keep the defaults.
var (
	Version = "dev"
	GitHash = "unknown"
)
