# kdb3
database written in Go with sqlite3 as storage and query/view engine. Not ready yet. doucmentation is incomplete and inprogress.

Features
  1. Document Database - Done
  2. Optimistic Concurrency - Done
  3. Restful API - Done
  4. Change tracking - Done
  4. Incrementally updated Materialistic View (with sqlite3) - Done
  5. Incremental Backup
  6. External Replication
  7. External Views
  8. UI - InProgress
 
# How does it works?
Let me assume you're familiar with sqlite3 database. To work with kdb3, its important. its a RDBMS database and does has support for json. https://www.sqlite.org/json1.html

Let's create sqlite database for our blog with a table name called “documents” with sample data. it has only title attribute to make my example simple.

    CREATE TABLE documents (data);  
    INSERT INTO documents (data) VALUES(‘{“title”:”getting started”}’);
    INSERT INTO documents (data) VALUES(‘{“title”:”kdb3 has full sql support”}’);

Let’s create another table called “posts”, to show list of posts.

    CREATE TABLE posts (title)
    INSERT INTO posts (title) SELECT json_extract(data, ‘$.title’) from documents

Now “posts” table has materialized the data which is in “documents” table. Based on access pattern, we just have optimized way to look at list of posts. 

If data changed in “documents” table, we have noway know what changed, so we have to delete and recreate all the rows in “posts” tables. What if we have change tracking, now we can delete/update only changed rows.kdb3 is just doing that. 

kdb3 is a document database and it has full support for SQL language. Let’s discuss about some of its concepts. Most of its design decisions are heavily inspired by couchdb. kdb3 uses sqlite3 as a storage/query engine and you can interact with it using HTTP rest api. kdb3 has two major components, database and its views. views are materialized. 















kdb3 database is a sqlite database file and it accepts only json documents, views are made with row/column based tables. materialized view is a just another sqlite database file.

You can create database as follows.

    curl -X PUT localhost:8001/blog
    {“ok”:true}

materialized view are great, since one can optimize the data for particular access pattern and make data access really fast. But materialized view is never refreshed util you wanted to, since RDBMS has no way to know which rows are changed, so it has to reprocess all the rows in order to update the materialized view with latest data.

kdb3 database accepts json objects and returns an id and version number. Id assigned, if not present in the document. Version number are used as optimistic concurrency locking, you always need latest version, in order to update the document. 

    curl -X POST localhost:8001/blog -d ‘{“title”:”getting started”}’
    {"_id":"f7f7a5d6d8d4b8292b346c83fd5fbbd7","_version":1}

    curl -X POST localhost:8001/blog -d ‘{“title”:”kdb3 is great”}’
    {"_id":"f7f7a5d6d8d4b8292b346c83fd5fbbd8","_version":1}

One can view the document back as follows

    curl -X GET localhost:8001/blog/f7f7a5d6d8d4b8292b346c83fd5fbbd7

A change tracking sequence number is also assigned on every document insertion and modification. With change tracking in place, one can ask what changed in the database from last time.

    curl -X GET localhost:8001/blog/_changes
    {
      "results": [
        {
          "seq": "PekcYXXlg_55f3i0o9utQ9271W5WFgDge742C4iWeFavCXxWh",
          "version": 1,
          "id": "f7f7a5d6d8d4b8292b346c83fd5fbbd8"
        },
        {
          "seq": "PekcYXXlg_55f3i0o9utQ9271W5WFgDge742C4iWeFavCXxWg",
          "version": 1,
          "id": "f7f7a5d6d8d4b8292b346c83fd5fbbd7"
        },
        {
          "seq": "PekcYXXlg_55f3i0o9utQ9271W5WFgDge742C4iWeFavCXxWf",
          "version": 1,
          "id": "_design/_views"
        }
      ]
    }

“_design/_views” is the default view definition, created on database creation. kdb3 materialized views uses change tracking sequence number to get latest data changes from its database and keep itself update to date. 











View engine, takes changed documents from databases and extract/compute data keep it update to date based on view definition. View definition is a bunch of sql statements, wrapped with json syntax.

Example view definition:

    {
      "_id": "_design/posts",
      "_version": 1,
      "_kind": "design",
      "views": {
        "_all_posts": {
          "setup": [
            "CREATE TABLE IF NOT EXISTS posts (title, doc_id, PRIMARY KEY(doc_id))"
          ],
          "run": [
            "DELETE FROM posts WHERE doc_id in (SELECT doc_id FROM latest_changes WHERE deleted = 1)",
            "INSERT OR REPLACE INTO posts (title, doc_id) SELECT json_extract(data, '$.title'), doc_id FROM latest_documents WHERE deleted = 0 AND json_extract(data, ‘$.title') is not null"
          ],
          "select": {
            "default": "SELECT JSON_OBJECT('rows',JSON_GROUP_ARRAY(JSON_OBJECT('title', title, 'id', doc_id))) FROM posts"
          }
        }
      }
    }

Note : POST following json document to same blog database.

Above view definition has enough information to build and keep data update to date. Let’s discuss in detail about view definition. Each view in the “views” section has 3 sections. “setup”, “run” and “select”.

“setup”  - set of SQL scripts to create view’s tables and indexes and etc. this scripts runs, whenever a connection open to view’s sqlite3 database.

“run”    - set of SQL scripts keep data update to date. DELETE statement is to delete documents, which are deleted. INSERT OR REPLACE is to insert/replace rows.

“select” - to select data out of view.

View can be executed with followings

    curl -X GET localhost:8001/blog/_design/posts/_all_posts
    {
      "rows": [
        {
          "title": "getting started",
          "id": "f7f7a5d6d8d4b8292b346c83fd5fbbd7"
        },
        {
          "title": "kdb3 is great",
          "id": "f7f7a5d6d8d4b8292b346c83fd5fbbd8"
        }
      ]
    }

## How to Build?

    go build -tags "json1 fts5" # cgo support required.
    ./kdb3 & # its running at port 8001

## create database

    curl localhost:8001/testdb -X PUT
    {"ok":true}

## delete database

    curl localhost:8001/testdb -X DELETE
    {"ok":true}

## basic ui

  http://localhost:8001/_utils

## database information

    curl localhost:8001/testdb -X GET
    {"db_name":"testdb","update_seq":"CxBnpvkllqAZmLSVYZX8YddwPF5bJr1K9IWdIbQMiWd1oDwTMCFYE_xPbpdsCzEOaKrEV1cRoiOQSbMzBt8IvC3cLc_YbJnCD9pb1xUAP1akELyyRnAOZkqjBvpRqXi5rUAlFbkfWV","doc_count":1}

## put documents

    curl localhost:8001/testdb -X POST -d '{}'
    {"_id":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx","_verison":1}

    curl localhost:8001/testdb -X POST -d '{"_id":1}'
    {"_id":"1","_verison":1}

    curl localhost:8001/testdb -X POST -d '{"_id":2,"name":"test"}'
    {"_id":"2","_verison":1}

## update documents

    curl localhost:8001/testdb -X POST -d '{"_id":2, "_version":1,"name":"test1"}'
    {"_id":"2","_verison":2}

## view documents
    
    curl localhost:8001/testdb/2 -X GET
    {"_id":"2","_verison":2,"name":"test1"}

## delete documents

    curl localhost:8001/testdb/1\?version=1
    {"_id":"1","_verison":2,"_deleted":true}

## changes 

    curl localhost:8001/testdb/_changes
    {
      "results": [
        {
          "seq": "CxBnpvkllqAZmLSVYZX8YddwPF5bJr1K9IWdIbQMiWd1oDwTMCFYE_xPbpdsCzEOaKrEV1cRoiOQSbMzBt8IvC3cLc_YbJnCD9pb1xUAP1akELyyRnAOZkqjBvpRqXi5rUAlFbkfW_",
          "version": 2,
          "id": "1",
          "deleted": 1
        },
        {
          "seq": "CxBnpvkllqAZmLSVYZX8YddwPF5bJr1K9IWdIbQMiWd1oDwTMCFYE_xPbpdsCzEOaKrEV1cRoiOQSbMzBt8IvC3cLc_YbJnCD9pb1xUAP1akELyyRnAOZkqjBvpRqXi5rUAlFbkfWZ",
          "version": 2,
          "id": "2"
        },
        {
          "seq": "CxBnpvkllqAZmLSVYZX8YddwPF5bJr1K9IWdIbQMiWd1oDwTMCFYE_xPbpdsCzEOaKrEV1cRoiOQSbMzBt8IvC3cLc_YbJnCD9pb1xUAP1akELyyRnAOZkqjBvpRqXi5rUAlFbkfWW",
          "version": 1,
          "id": "62bdf735b65cb9de2e0c63ceee5fbbd7"
        },
        {
          "seq": "CxBnpvkllqAZmLSVYZX8YddwPF5bJr1K9IWdIbQMiWd1oDwTMCFYE_xPbpdsCzEOaKrEV1cRoiOQSbMzBt8IvC3cLc_YbJnCD9pb1xUAP1akELyyRnAOZkqjBvpRqXi5rUAlFbkfWV",
          "version": 1,
          "id": "_design/_views"
        }
      ]
    }

## incrementally updated materialistic View

### to view, view definitions

    curl localhost:8001/testdb/_design/_views -X GET 
    {
      "_id": "_design/_views",
      "_version": 1,
      "_kind": "design",
      "views": {
        "_all_docs": {
          "setup": [
            "CREATE TABLE IF NOT EXISTS all_docs (key, value, doc_id,  PRIMARY KEY(key)) WITHOUT ROWID"
          ],
          "exec": [
            "DELETE FROM all_docs WHERE doc_id in (SELECT doc_id FROM latest_changes WHERE deleted = 1)",
            "INSERT OR REPLACE INTO all_docs (key, value, doc_id) SELECT doc_id, JSON_OBJECT('version', version), doc_id FROM latest_documents WHERE deleted = 0"
          ],
          "select": {
            "default": "SELECT JSON_OBJECT('offset', min(offset),'rows',JSON_GROUP_ARRAY(JSON_OBJECT('key', key, 'value', JSON(value), 'id', doc_id)),'total_rows',(SELECT COUNT(1) FROM all_docs)) FROM (SELECT (ROW_NUMBER() OVER(ORDER BY key) - 1) as offset, * FROM all_docs ORDER BY key) WHERE (${key} IS NULL or key = ${key})",
            "with_docs": "SELECT JSON_OBJECT('offset', min(offset),'rows',JSON_GROUP_ARRAY(JSON_OBJECT('id', doc_id, 'key', key, 'value', JSON(value), 'doc', JSON((SELECT data FROM documents WHERE doc_id = o.doc_id)))),'total_rows',(SELECT COUNT(1) FROM all_docs)) FROM (SELECT (ROW_NUMBER() OVER(ORDER BY key) - 1) as offset, * FROM all_docs ORDER BY key) o WHERE (${key} IS NULL or key = ${key})"
          }
        }
      }
    }

### execute view

    curl localhost:8001/testdb/_design/_views/_all_docs/default -X GET 
    {
      "offset": 0,
      "rows": [
        {
          "key": "2",
          "value": {
            "version": 2
          },
          "id": "2"
        },
        {
          "key": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
          "value": {
            "version": 1
          },
          "id": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        },
        {
          "key": "_design/_views",
          "value": {
            "version": 1
          },
          "id": "_design/_views"
        }
      ],
      "total_rows": 3
    }

[![asciicast](https://asciinema.org/a/GwSJcYRffxpTph59CLeTKYkmX.svg)](https://asciinema.org/a/GwSJcYRffxpTph59CLeTKYkmX)
