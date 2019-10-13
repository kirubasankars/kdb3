
#include <stdio.h>

#include "sqlite3.h"
#include "kdb.h"

int kouchdb_open(char *name, kouchdb **ppDB)
{
    kouchdb *kdb;
    int rc = 0;
    char *errmsg = 0;

    kdb = sqlite3_malloc(sizeof(kouchdb));
    int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_EXCLUSIVE;
    rc = sqlite3_open_v2(name, &kdb->db, flags, NULL);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "can't open database: %s\n", sqlite3_errmsg(kdb->db));
        sqlite3_close(kdb->db);
    }

    rc = sqlite3_exec(kdb->db, "PRAGMA journal_mode=WAL;", NULL, NULL, &errmsg);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "can't open database: %s\n", sqlite3_errmsg(kdb->db));
        sqlite3_close(kdb->db);
    }

    rc = sqlite3_prepare_v3(kdb->db, "SELECT sqlite_version()", -1, SQLITE_PREPARE_NORMALIZE, &kdb->stmt_version, NULL);
    if (rc != SQLITE_OK)
    {
        fprintf(stderr, "can't open database: %s\n", sqlite3_errmsg(kdb->db));
        sqlite3_close(kdb->db);
    }

    *ppDB = kdb;

    return 0;
}

int kouchdb_close(kouchdb *pDB)
{
    sqlite3_finalize(pDB->stmt_version);

    sqlite3_close_v2(pDB->db);
    sqlite3_free(pDB);

    return 0;
}

char *kouchdb_version(kouchdb *pDB)
{
    int rc = 0;
    char *version = NULL;

    rc = sqlite3_step(pDB->stmt_version);
    if (rc == SQLITE_ROW)
    {
        version = (char *)sqlite3_column_text(pDB->stmt_version, 0);
    }
    sqlite3_reset(pDB->stmt_version);

    return version;
}