# Upgrading sqlite

We need to make our own amalgamation, since we want to enable
`SQLITE_ENABLE_UPDATE_DELETE_LIMIT` during the parser generator phase.

`-DSQLITE_ENABLE_UPDATE_DELETE_LIMIT=1` seems to be the only important option when
creating the amalgamation.

From the repository root:

```sh
make sqlite-amalgamation
```

Or:

```sh
./sqlite3/upgrade_sqlite.sh
```

Override version/year if needed:

```sh
SQLITE_VERSION=3.53.4 SQLITE_YEAR=2026 ./sqlite3/upgrade_sqlite.sh
```
