# kdb3 ![Go](https://github.com/kirubasankars/kdb3/workflows/Go/badge.svg)

Document database written in Go with SQLite as storage and query/view engine.

## Features

1. Document database with optimistic concurrency (`_rev`)
2. RESTful HTTP API
3. Change tracking (`_changes`, including continuous SSE `feed=eventsource`)
4. Incrementally updated materialized views (SQL over SQLite)
5. Live vacuum
6. Admin UI at `/_utils/`
7. Swagger UI at `/_docs/`
8. Optional Bearer-token auth
9. Prometheus metrics at `/metrics` (always public)

**Trust model:** design documents run SQL against SQLite. Do not expose kdb3 on an untrusted network without a token and a restricted bind address. Default listen address is `127.0.0.1:8001`.

## Contents

- [Build](#build)
- [Run](#run)
- [Test](#test)
- [Development](#development) — prerequisites, common tasks, project layout
- [Quick start](#quick-start)
- [Naming rules](#naming-rules)
- [HTTP API](#http-api)
- [SQLite durability](#sqlite-durability)
- [Releases](#releases)
- [License](#license)

## Build

Requires Go 1.22+ and CGO (for the in-tree SQLite driver).

```sh
make build          # produces ./kdb3, stamps VERSION + git hash
# or: go build -ldflags "-X main.Version=$(cat VERSION) -X main.GitHash=$(git rev-parse --short HEAD)" -o kdb3 .
./kdb3 -version     # kdb3 1.0.0 (abc1234)
```

SQLite is provided by [`sqlite3/`](sqlite3/) (fork of go-sqlite-lite) with amalgamation **3.53.4**. Regenerate with `make sqlite-amalgamation` (see [`sqlite3/README.md`](sqlite3/README.md)).

Prebuilt binaries for Linux and macOS (`amd64` / `arm64`) are attached to [GitHub Releases](https://github.com/kirubasankars/kdb3/releases). Each asset is `kdb3-vVERSION-OS-ARCH.tar.gz` plus `SHA256SUMS`.

## Run

```sh
./kdb3
# Listening on 127.0.0.1:8001 (data=./data)
```

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `127.0.0.1:8001` | HTTP listen address |
| `-data` | `./data` | Data directory for databases and views |
| `-token` | _(empty)_ | Bearer token; empty disables auth. Env `KDB3_TOKEN` used if flag is empty |
| `-read-timeout` | `60s` | HTTP read timeout |
| `-write-timeout` | `60s` | HTTP write timeout |
| `-version` | `false` | Print stamped version and git hash, then exit |

```sh
make run                    # build and run
make run ADDR=0.0.0.0:8001 TOKEN=secret
```

With auth enabled:

```sh
./kdb3 -token secret
curl -H 'Authorization: Bearer secret' http://127.0.0.1:8001/_cat/dbs
```

Admin UI assets under `share/www/` are embedded via `go:embed` (no Node build). Open http://127.0.0.1:8001/_utils/ — edit the UI, then rebuild `kdb3`. The UI shell is public; API calls still require the token when auth is enabled (paste it in the UI).

Interactive API docs (Swagger UI + OpenAPI) are at http://127.0.0.1:8001/_docs/ — sources under `share/openapi/`, also embedded. Use **Authorize** with your Bearer token when the server has `-token` set.

Prometheus metrics (Go runtime, HTTP, and kdb3 app metrics) are at http://127.0.0.1:8001/metrics and stay public even when `-token` is set:

```sh
curl http://127.0.0.1:8001/metrics
```

## Test

```sh
make ci      # build, test, and race (same as GitHub Actions)
make test    # go test ./...
make race    # go test -race ./...
make bench   # go test -bench=. -benchmem ./...
make cover   # coverage summary
```

Optional HTTP integration coverage (needs Python): `pip install requests pytest`, start `./kdb3`, then run `pytest test_main.py`.

## Development

### Prerequisites

- **Go 1.22+** with **CGO enabled** — the in-tree SQLite driver is C code
- A **C compiler** (`gcc` or `clang`) plus standard build tools
- **`make`** for the helper targets below
- _(optional)_ **Python 3** with `requests` + `pytest` for the HTTP integration suite

These are standard on Linux/macOS. On a fresh Debian/Ubuntu box: `sudo apt-get install -y build-essential`. Cloud Agents boot from [`.cursor/environment.json`](.cursor/environment.json), which builds the binary and starts the server automatically.

### Common tasks

| Command | What it does |
|---------|--------------|
| `make build` | Build `./kdb3` (stamps VERSION + git hash) |
| `make run` | Build and run (`ADDR` / `DATA` / `TOKEN` overrides) |
| `make test` | `go test ./...` |
| `make race` | `go test -race ./...` |
| `make ci` | `build` + `test` + `race` — the same gate GitHub Actions runs |
| `make bench` | Benchmarks with allocation stats |
| `make cover` | Coverage summary |
| `make clean` | Remove the binary, coverage, and `./data` |

Run `make help` for the full list. Before opening a pull request, run `make ci` — it mirrors the [`Go` workflow](.github/workflows/go.yml).

### Project layout

| Path | Responsibility |
|------|----------------|
| `main.go`, `config.go` | Process entry point and flag/env configuration |
| `routes.go`, `handlers.go`, `auth.go` | HTTP router, request handlers, and bearer-token middleware |
| `kdb.go`, `service_locator.go` | Engine top level: database registry, wiring, lifecycle |
| `db.go`, `db_reader.go`, `db_writer.go`, `local_db.go` | Per-database storage over SQLite |
| `document.go`, `sequence.go`, `model.go` | Document model and `_rev` / change-sequence bookkeeping |
| `mrview*.go` | Materialized views: SQL `setup` / `run` / `select`, incremental update, and the view atelier |
| `vacuum_manager.go` | Live vacuum |
| `metrics.go` | Prometheus instrumentation |
| `errors.go`, `utils.go`, `filehandler.go` | Shared error types and helpers |
| `ui_embed.go`, `share/www/` | Embedded Admin UI served at `/_utils/` |
| `share/openapi/` | Embedded Swagger UI + OpenAPI spec served at `/_docs/` |
| `share/js/follow.js` | Browser helper for the authenticated `_changes` SSE feed |
| `sqlite3/` | Vendored SQLite driver + amalgamation (see [`sqlite3/README.md`](sqlite3/README.md)) |
| `*_test.go`, `test_main.py` | Go unit/race/bench tests and optional Python HTTP integration tests |

## Quick start

Create a database:

```sh
curl -X PUT http://127.0.0.1:8001/blog
# {"ok":true}
```

Write documents (JSON body; `Content-Type: application/json` required). An `_id` is assigned if omitted. `_rev` is the optimistic concurrency token — updates and deletes need the latest revision.

```sh
curl -X POST http://127.0.0.1:8001/blog \
  -H 'Content-Type: application/json' \
  -d '{"title":"getting started"}'
# {"_id":"…","_rev":1}

curl -X POST http://127.0.0.1:8001/blog \
  -H 'Content-Type: application/json' \
  -d '{"_id":"1","title":"kdb3 is great"}'
# {"_id":"1","_rev":1}

curl -X POST http://127.0.0.1:8001/blog \
  -H 'Content-Type: application/json' \
  -d '{"_id":"1","_rev":1,"title":"kdb3 is great"}'
# {"_id":"1","_rev":2}

curl -X GET http://127.0.0.1:8001/blog/1
# {"_id":"1","_rev":2,"title":"kdb3 is great"}
```

Every insert/update gets a change-tracking sequence. `_changes` is a timeline of the database:

```sh
curl 'http://127.0.0.1:8001/blog/_changes'
curl 'http://127.0.0.1:8001/blog/_changes?since=2&limit=100'
```

## Naming rules

- **Database names:** `[a-z0-9_]`, length 1–50, must not start with `_`
- **Document IDs:** `[A-Za-z0-9_]`, length ≤ 50, must not start with `_` (except `_design/…`)
- **Design doc IDs:** `_design/` + same character class, name length ≤ 50

## HTTP API

- Interactive docs: http://127.0.0.1:8001/_docs/ ([`share/openapi/openapi.yaml`](share/openapi/openapi.yaml))
- Full route list: [`routes.go`](routes.go)

### Server

```sh
curl http://127.0.0.1:8001/                 # {"name":"kdb3","version":{"kdb3":"…","commit":"…","sqlite":"…"}}
curl http://127.0.0.1:8001/_cat/dbs         # ["blog","testdb"]
curl 'http://127.0.0.1:8001/_uuids?count=3' # ["…","…","…"]
```

### Databases

```sh
curl -X PUT    http://127.0.0.1:8001/testdb
curl -X GET    http://127.0.0.1:8001/testdb
# {"name":"testdb","update_seq":1,"doc_count":1,"deleted_doc_count":0}

curl -X DELETE http://127.0.0.1:8001/testdb
curl -X POST   http://127.0.0.1:8001/testdb/_vacuum
# {"ok":true}
```

Creating a database also creates the default design doc `_design/_views` with an `_all_docs` view.

### Documents

```sh
# Create / update (POST to DB or PUT by id)
curl -X POST http://127.0.0.1:8001/testdb \
  -H 'Content-Type: application/json' -d '{"_id":"2","name":"test"}'
curl -X PUT  http://127.0.0.1:8001/testdb/2 \
  -H 'Content-Type: application/json' -d '{"_id":"2","_rev":1,"name":"test1"}'

curl -X GET  http://127.0.0.1:8001/testdb/2
curl -I      http://127.0.0.1:8001/testdb/2          # E-Tag = revision
curl -X DELETE 'http://127.0.0.1:8001/testdb/2?rev=2'
# {"_id":"2","_rev":3,"_deleted":true}
```

Request bodies are capped at 1 MiB. Writes require `Content-Type: application/json`.

### Bulk

```sh
curl -X POST http://127.0.0.1:8001/testdb/_bulk_docs \
  -H 'Content-Type: application/json' \
  -d '{"_docs":[{"_id":"a"},{"_id":"b","name":"x"}]}'
# [{"_id":"a","_rev":1},{"_id":"b","_rev":1}]

curl -X POST http://127.0.0.1:8001/testdb/_bulk_gets \
  -H 'Content-Type: application/json' \
  -d '{"_docs":[{"_id":"a"},{"_id":"missing"}]}'
```

Each array element is either a document result or `{"_id":"…","error":"…","reason":"…"}`.

Atomic batches (`all_or_nothing: true`) commit every document or none. On failure the server returns HTTP `409` with `{"error":"bulk_failed","reason":"…","results":[…]}` and leaves the database unchanged.

```sh
curl -X POST http://127.0.0.1:8001/testdb/_bulk_docs \
  -H 'Content-Type: application/json' \
  -d '{"all_or_nothing":true,"_docs":[{"_id":"a"},{"_id":"b","_rev":99}]}'
# HTTP 409 — no documents committed
```

### Changes

One-shot (poll):

```sh
curl 'http://127.0.0.1:8001/testdb/_changes?since=0&limit=100&descending=true'
# {"results":[...],"last_seq":N}
```

Continuous SSE feed:

```sh
curl -N 'http://127.0.0.1:8001/testdb/_changes?feed=eventsource&since=0'
# data: {"update_seq":1,"id":"…","rev":1}
# :            (heartbeat comment ~every 25s)
```

| Query | Default | Description |
|-------|---------|-------------|
| `since` | `0` | Return changes with `update_seq` greater than this |
| `limit` | `1000` | Max results (per batch for SSE) |
| `descending` | `false` | Newest first when `true` (ignored for `feed=eventsource`) |
| `feed` | `normal` | `normal` (JSON) or `eventsource` (SSE stream) |

JS helper ([`share/js/follow.js`](share/js/follow.js)) — uses `fetch` + stream so Bearer auth works (native `EventSource` cannot set `Authorization`):

```js
const { follow } = kdb3Follow; // or require('./share/js/follow.js')
const cache = new Map();
const sub = follow(
  { url: 'http://127.0.0.1:8001', db: 'testdb', since: 0, token: process.env.KDB3_TOKEN },
  (change) => {
    if (change.deleted) cache.delete(change.id);
    else cache.set(change.id, change);
  }
);
// sub.since(); sub.abort();
```

### All docs

Shortcut over `_design/_views/_all_docs`:

```sh
curl 'http://127.0.0.1:8001/testdb/_all_docs?page=1&limit=10&include_docs=true'
```

| Query | Default | Description |
|-------|---------|-------------|
| `page` | `1` | 1-based page |
| `limit` | `10` | Page size |
| `include_docs` | `false` | Include full documents |

### Design documents and views

`_design/_views` is created with every database. Views use SQL: `setup` (DDL on open), `run` (incremental update from `latest_changes` / `latest_documents`), and `select` (named queries).

Example custom view (`post_view.json`):

```json
{
  "_id": "_design/posts",
  "views": {
    "_all_posts": {
      "setup": [
        "CREATE TABLE IF NOT EXISTS posts (title, doc_id, PRIMARY KEY(doc_id))"
      ],
      "run": [
        "DELETE FROM posts WHERE doc_id in (SELECT doc_id FROM latest_changes WHERE deleted = 1)",
        "INSERT OR REPLACE INTO posts (title, doc_id) SELECT json_extract(data, '$.title'), doc_id FROM latest_documents WHERE deleted = 0 AND json_extract(data, '$.title') is not null"
      ],
      "select": {
        "default": "SELECT JSON_OBJECT('rows',JSON_GROUP_ARRAY(JSON_OBJECT('title', title, 'id', doc_id))) FROM posts"
      }
    }
  }
}
```

```sh
curl -X POST http://127.0.0.1:8001/blog \
  -H 'Content-Type: application/json' -d @./post_view.json

curl http://127.0.0.1:8001/blog/_design/posts
curl http://127.0.0.1:8001/blog/_design/posts/_all_posts
curl http://127.0.0.1:8001/blog/_design/posts/_all_posts/default
curl 'http://127.0.0.1:8001/blog/_design/posts/_all_posts?stale=true'
```

Full-text search with **FTS5** (compiled into kdb3’s SQLite). Project document fields into a virtual table in `setup`/`run`, then match in `select`. Pass the query as a form/query param (e.g. `?q=hello`):

```json
{
  "_id": "_design/search",
  "views": {
    "_posts_fts": {
      "setup": [
        "CREATE VIRTUAL TABLE IF NOT EXISTS posts_fts USING fts5(title, body, doc_id UNINDEXED)"
      ],
      "run": [
        "DELETE FROM posts_fts WHERE doc_id IN (SELECT doc_id FROM latest_changes)",
        "INSERT INTO posts_fts (title, body, doc_id) SELECT json_extract(data, '$.title'), json_extract(data, '$.body'), doc_id FROM latest_documents WHERE deleted = 0 AND json_extract(data, '$.title') IS NOT NULL"
      ],
      "select": {
        "default": "SELECT JSON_OBJECT('rows', JSON_GROUP_ARRAY(JSON_OBJECT('id', doc_id, 'title', title))) FROM posts_fts WHERE posts_fts MATCH ${q} ORDER BY rank LIMIT CAST(ifnull(${limit}, 10) AS INT)"
      }
    }
  }
}
```

```sh
curl -X POST http://127.0.0.1:8001/blog \
  -H 'Content-Type: application/json' -d @./search_view.json

curl 'http://127.0.0.1:8001/blog/_design/search/_posts_fts/default?q=hello'
```

To unnest JSON arrays in `run`/`select`, use `json_each` / `json_tree` (JSON1 is also built in), for example `json_each(json_extract(data, '$.tags'))`.

| Query | Description |
|-------|-------------|
| `stale` | If `true`, read without waiting for the view to catch up |
| `include_docs` | Uses select name `{name}_with_docs` when present |

Other form values are available to select SQL as `${param}` placeholders (e.g. `${limit}`, `${key}`, `${q}`).
Default `_all_docs` definition (for reference):

```json
{
  "_id": "_design/_views",
  "views": {
    "_all_docs": {
      "setup": [
        "CREATE TABLE IF NOT EXISTS all_docs (key, rev, doc_id, PRIMARY KEY(doc_id)) WITHOUT ROWID",
        "CREATE TABLE IF NOT EXISTS all_docs_meta (id INTEGER PRIMARY KEY, total_rows INTEGER) WITHOUT ROWID",
        "INSERT OR IGNORE INTO all_docs_meta (id, total_rows) VALUES (1, 0)"
      ],
      "run": [
        "DELETE FROM all_docs WHERE doc_id in (SELECT doc_id FROM latest_changes WHERE deleted = 1)",
        "INSERT OR REPLACE INTO all_docs (key, rev, doc_id) SELECT doc_id, rev, doc_id FROM latest_documents WHERE deleted = 0",
        "UPDATE all_docs_meta SET total_rows = (SELECT COUNT(1) FROM all_docs) WHERE id = 1"
      ],
      "select": {
        "default": "SELECT JSON_OBJECT('offset', ifnull(min(offset) + 1, 0),'rows', JSON_GROUP_ARRAY(JSON_OBJECT('key', doc_id, 'id', doc_id, 'rev', rev)),'total_rows', (SELECT total_rows FROM all_docs_meta WHERE id = 1)) as data FROM (SELECT (ROW_NUMBER() OVER(ORDER BY doc_id) - 1) as offset, * FROM all_docs WHERE (${startkey} IS NULL OR doc_id >= ${startkey}) AND (${endkey} IS NULL OR doc_id <= ${endkey}) ORDER BY doc_id LIMIT CAST(${limit} AS INT) OFFSET CAST(${offset} AS INT))",
        "with_docs": "SELECT JSON_OBJECT('offset', ifnull(min(offset) + 1, 0),'rows', JSON_GROUP_ARRAY(JSON_OBJECT('key', doc_id, 'id', doc_id, 'rev', rev, 'doc', JSON((SELECT data FROM documents WHERE doc_id = o.doc_id)))),'total_rows', (SELECT total_rows FROM all_docs_meta WHERE id = 1)) as data FROM (SELECT (ROW_NUMBER() OVER(ORDER BY doc_id) - 1) as offset, * FROM all_docs WHERE (${startkey} IS NULL OR doc_id >= ${startkey}) AND (${endkey} IS NULL OR doc_id <= ${endkey}) ORDER BY doc_id LIMIT CAST(${limit} AS INT) OFFSET CAST(${offset} AS INT)) o"
      }
    }
  }
}
```

## SQLite durability

Databases open with `journal_mode=WAL` and `synchronous=NORMAL` (plus `busy_timeout=5000` and a 64MB page cache). `NORMAL` is faster than `FULL`, but after a hard power loss the last WAL transactions may need recovery; a clean process restart is fine. Prefer an UPS and/or periodic `POST /{db}/_vacuum` for stronger operational hygiene.

## Releases

Version is the contents of [`VERSION`](VERSION). `make build` embeds it and the current git hash (`./kdb3 -version` and `GET /`).

To cut a release:

1. Set `VERSION` (for example `1.0.0`) and commit.
2. `make tag` — creates annotated `v1.0.0` on a clean tree.
3. `git push origin master && git push origin v1.0.0`

Pushing `v*` runs [`.github/workflows/release.yml`](.github/workflows/release.yml): tests, a GitHub Release, and CGO binaries for `linux`/`darwin` × `amd64`/`arm64`.

## License

See [LICENSE](LICENSE).
