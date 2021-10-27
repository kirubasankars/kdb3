# kdb3 ![Go](https://github.com/kirubasankars/kdb3/workflows/Go/badge.svg)
database written in Go with sqlite3 as storage and query/view engine. documentation is incomplete and in-progress.

Features
  1. Document Database
  2. Optimistic Concurrency (optional)
  3. Restful API
  4. Change tracking
  5. Incrementally updated Materialized View (with sqlite3)
  6. Live Vacuum
  7. UI - InProgress

Database can be created as follows.

    curl -X PUT localhost:8001/blog
    {“ok”:true}

database accepts json object and returns an id and version number. ID assigned, if not present in the document. 
Version number used as optimistic concurrency locking, you need the latest version, in order to update the document. if you omit it, database assign latest version number 

    curl -X POST localhost:8001/blog -d ‘{“title”:”getting started”}’
    {"_id":"f7f7a5d6d8d4b8292b346c83fd5fbbd7","_version":1}

    curl -X POST localhost:8001/blog -d ‘{"_id", "1", “title”:”kdb3 is great”}’
    {"_id":"1","_version":1}
    
    curl -X POST localhost:8001/blog -d ‘{"_id", "1", “title”:”kdb3 is great”}’
    {"_id":"1","_version":2}

Document can be retrieved back as follows

    curl -X GET localhost:8001/blog/1

On every document insert and update, change tracking sequence number assigned. "_changes" api works like timeline on the database. it can help to get document changes in sequence.

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

 One can ask what changed in the database from last time.
 
    curl -X GET localhost:8001/blog/_changes?since=<seq_number>

“_design/_views” is the default view, created on database creation. kdb3 supports materialized view and uses change tracking sequence number to get the latest data changes from its database and keep itself update to date. 

View engine, takes changed documents from the database and extract/compute data changes and keep the view, update to date based on view definition. View definition is a bunch of sql statements, wrapped with json syntax.

Example view definition:

    {
      "_id": "_design/posts",
      "_rev": "1-",
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

Above view definition has necessary information to build and keep the data update to date. Let’s discuss in detail about view definition. Each view in the “views” list has 3 sections. “setup”, “run” and “select”.

“setup”  - set of SQL scripts to create view’s tables and indexes, etc. this scripts runs, whenever a connection open to view’s sqlite3 database.

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

## incrementally updated materialized View

### to view, view definitions

    curl localhost:8001/testdb/_design/_views -X GET 
    {
      "_id": "_design/_views",
      "_version": 1,
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
