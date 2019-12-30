# kdb3
database written in Go with sqlite3 as storage and query/view engine. Not ready yet.

Features
  1. Document Database - Done
  2. Optimistic Concurrency - Done
  3. Restful API - Done
  3. Change tracking - Done
  4. Incrementally updated Materialistic View (with sqlite3) - Done
  5. Incremental Backup
  6. External Replication
  7. External Views
  8. Cluster
  9. High Availability with replica
 10. UI - InProgress
 
# How does it works?
  whole system of kdb3 build on top change tracking system. 
  
  POST /(database) takes objects as json documents and store it in sqlite database table with (incremental) change tracking sequence number in time. With this change tracking sequence, one can ask database what changed from last change sequence.
 
 Ex: GET /(database)/_changes?since=(seq)
 
 Let's discuss about Incrementally updated Materialistic View
 
 Views in KDB3, is just another sqlite database. 
 
 You properbly knows about materialistic view from popular RDBMS databases. kdb3 has incrementally updated materialistic view, since kdb3 has change tracking system. one can build a view just as post speical kind of document called view document as follows.
 
 Ex: 
 
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
            "default": "SELECT JSON_OBJECT('offset', min(offset),'rows',JSON_GROUP_ARRAY(JSON_OBJECT('key', key, 'value', JSON(value), 'id', doc_id)),'total_rows',(SELECT COUNT(1) FROM all_docs)) FROM (SELECT (ROW_NUMBER() OVER(ORDER BY key) - 1) as offset, * FROM all_docs ORDER BY key) WHERE (${key} IS NULL or key = ${key})"
          }
        }
      }
    }
    

Above example view document has instructions to create tables, sync up the data and named select stmts. data is inserted/updated, when one trying to accessing that view. when view defination changes, view will be rebuild next view request.

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
