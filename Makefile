BINARY := kdb3
ADDR   ?= 127.0.0.1:8001
DATA   ?= ./data
TOKEN  ?=

# Silence harmless sqlite3.c amalgamation warnings from the vendored driver.
export CGO_CFLAGS ?= -Wno-implicit-const-int-float-conversion

.PHONY: all build test race ci bench cover run clean help sqlite-amalgamation

all: build

build:
	go build -o $(BINARY) .

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

clean:
	rm -f $(BINARY) coverage.out
	rm -rf $(DATA)

help:
	@echo "Targets:"
	@echo "  make build              # build ./kdb3 (CGO required)"
	@echo "  make test               # go test ./..."
	@echo "  make race               # go test -race ./..."
	@echo "  make ci                 # build, test, and race (GitHub Actions gate)"
	@echo "  make bench              # go test -bench=. -benchmem ./..."
	@echo "  make cover              # test with coverage summary"
	@echo "  make run                # build and run (ADDR/DATA/TOKEN env overrides)"
	@echo "  make sqlite-amalgamation # regenerate sqlite3.c/h (SQLite 3.53.4)"
	@echo "  make clean              # remove binary, coverage, and data dir"
