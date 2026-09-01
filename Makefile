BINARY  := kdb3
ADDR    ?= 127.0.0.1:8001
DATA    ?= ./data
TOKEN   ?=
VERSION ?= $(shell cat VERSION 2>/dev/null || echo dev)
GIT_HASH ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
LDFLAGS ?= -s -w -X main.Version=$(VERSION) -X main.GitHash=$(GIT_HASH)

# Silence harmless sqlite3.c amalgamation warnings from the vendored driver.
export CGO_CFLAGS ?= -Wno-implicit-const-int-float-conversion

.PHONY: all build test race ci bench cover run clean help sqlite-amalgamation tag

all: build

build:
	go build -ldflags "$(LDFLAGS)" -o $(BINARY) .

test:
	go test ./...

race:
	go test -race ./...

ci:
	$(MAKE) build
	$(MAKE) test
	$(MAKE) race

bench:
	go test -bench=. -benchmem ./...

cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

run: build
	./$(BINARY) -addr $(ADDR) -data $(DATA) $(if $(TOKEN),-token $(TOKEN),)

# Rebuild sqlite3/sqlite3.c and sqlite3.h from SQLite 3.53.4 sources.
sqlite-amalgamation:
	./sqlite3/upgrade_sqlite.sh

# Annotated tag v$(VERSION) from VERSION. Push to origin to publish a GitHub Release.
tag:
	@test -n "$(VERSION)" || { echo "VERSION is empty"; exit 1; }
	@git diff --quiet && git diff --cached --quiet || { echo "working tree not clean"; exit 1; }
	@if git rev-parse "v$(VERSION)" >/dev/null 2>&1; then echo "tag v$(VERSION) already exists"; exit 1; fi
	git tag -a "v$(VERSION)" -m "kdb3 v$(VERSION)"
	@echo "created tag v$(VERSION); publish with: git push origin v$(VERSION)"

clean:
	rm -f $(BINARY) coverage.out
	rm -rf $(DATA)

help:
	@echo "Targets:"
	@echo "  make build              # build ./kdb3 (CGO required; stamps VERSION + git hash)"
	@echo "  make test               # go test ./..."
	@echo "  make race               # go test -race ./..."
	@echo "  make ci                 # build, test, and race (GitHub Actions gate)"
	@echo "  make bench              # go test -bench=. -benchmem ./..."
	@echo "  make cover              # test with coverage summary"
	@echo "  make run                # build and run (ADDR/DATA/TOKEN env overrides)"
	@echo "  make tag                # annotated git tag v\$$(VERSION) from VERSION file"
	@echo "  make sqlite-amalgamation # regenerate sqlite3.c/h (SQLite 3.53.4)"
	@echo "  make clean              # remove binary, coverage, and data dir"
	@echo "Variables: VERSION=$(VERSION) GIT_HASH=$(GIT_HASH)"
