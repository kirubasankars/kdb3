# sqlite3

Vendored and maintained fork of [bvinc/go-sqlite-lite](https://github.com/bvinc/go-sqlite-lite) v0.6.1 for kdb3.

- **Module path:** `kdb3/sqlite3`
- **Amalgamation:** SQLite 3.53.4 (`sqlite3.c` / `sqlite3.h`)
- **License:** BSD-3-Clause (see [LICENSE](LICENSE))
- **CGO:** required; `Makefile` sets `CGO_CFLAGS=-Wno-implicit-const-int-float-conversion` to quiet amalgamation warnings

## Regenerate amalgamation

`SQLITE_ENABLE_UPDATE_DELETE_LIMIT` must be enabled while generating the amalgamation (not only at compile time). Details: [upgrading.md](upgrading.md).

From the repo root:

```sh
make sqlite-amalgamation
```

Or run [`upgrade_sqlite.sh`](upgrade_sqlite.sh) directly. Requires network access, `unzip`, and a C toolchain.

```sh
SQLITE_VERSION=3.53.4 SQLITE_YEAR=2026 ./sqlite3/upgrade_sqlite.sh
```
