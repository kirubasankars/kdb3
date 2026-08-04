# sqlite3

Vendored and maintained fork of [bvinc/go-sqlite-lite](https://github.com/bvinc/go-sqlite-lite) v0.6.1 for kdb3.

- **Module path:** `kdb3/sqlite3`
- **Amalgamation:** SQLite 3.53.4 (`sqlite3.c` / `sqlite3.h`)
- **License:** BSD-3-Clause (see [LICENSE](LICENSE))
- **CGO:** required; `Makefile` sets `CGO_CFLAGS=-Wno-implicit-const-int-float-conversion` to quiet amalgamation warnings

## Built-in SQLite features

Compile flags in [`sqlite3.go`](sqlite3.go) enable (among others):

- **JSON1** — `json_extract`, `json_each`, `json_tree`, `JSON_OBJECT`, …
- **FTS5** — full-text virtual tables for design-doc views
- **Math functions** — `log`, `pow`, `pi`, … (`SQLITE_ENABLE_MATH_FUNCTIONS`)
- **RTree / Geopoly** — spatial indexes when documents carry geometry
- **Session / STAT4 / Soundex**

**Loadable extensions are disabled** (`SQLITE_OMIT_LOAD_EXTENSION=1`). Design documents run SQL against the view database; runtime `.load` / `load_extension()` would allow arbitrary native code. Prefer the compiled-in features above (or additional static compile-ins), not downloadable `.so` plugins.

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
