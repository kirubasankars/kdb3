#include "sqlite3.h"

typedef struct
{
    sqlite3 *db;

    sqlite3_stmt *stmt_version;

} kouchdb;

int kouchdb_open(char * name, kouchdb **ppDB);
int kouchdb_close(kouchdb *pDB);

char *kouchdb_version(kouchdb *pDB);