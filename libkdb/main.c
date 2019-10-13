#include <stdio.h>
#include "kdb.h"

int main() {
    printf("Hi Kiruba!\n");
    kouchdb *kdb;
    kouchdb_open("dd.db", &kdb);
    printf("%s\n",kouchdb_version(kdb));
    kouchdb_close(kdb);
    return 0;
}