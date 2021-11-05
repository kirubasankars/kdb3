# kdb3 ![Go](https://github.com/kirubasankars/kdb3/workflows/Go/badge.svg)
database written in Go with sqlite3 as storage and query/view engine. documentation is incomplete and in-progress.

Features
  1. Document Database
  2. Optimistic Concurrency
  3. Restful API
  4. Change tracking
  5. Incrementally updated Materialized View (with sqlite3)
  6. Live Vacuum
  7. UI - InProgress

Database can be created as follows.

    curl -X PUT localhost:8001/blog
    {“ok”:true}

database accepts json object and returns an id and revision. ID assigned, if not present in the document. 
revision used as optimistic concurrency locking, you need the latest version, in order to update the document.

    curl -X POST localhost:8001/blog -d '{"title":"getting started"}'
    {"_id":"d64b73f378ed9dd1c3f9a4b3485fbbd7","_rev":"1-01e7d11edebeb23a9b9df7e2a56f1ad3"}

    curl -X POST localhost:8001/blog -d '{"_id":"1", "title":"kdb3 is great"}'
    {"_id":"1","_rev":"1-cd010b709ed5a0932f23c2ea13c4c5cf"}
    
    curl -X POST localhost:8001/blog -d '{"_id":"1", "_rev":"1-cd010b709ed5a0932f23c2ea13c4c5cf", "title":"kdb3 is great"}'
    {"_id":"1","_rev":"2-cd010b709ed5a0932f23c2ea13c4c5cf"}

Document can be retrieved back as follows

    curl -X GET localhost:8001/blog/1
    {"_id":"1","_rev":"2-cd010b709ed5a0932f23c2ea13c4c5cf","title":"kdb3 is great"}

On every document insert and update, change tracking sequence number assigned. "_changes" api works like timeline on the database. it can help to get document changes in sequence.

    curl -X GET localhost:8001/blog/_changes
    {
        "results": [
        {
          "seq": "MGo_tmXV8oawIMsTJta_u1HgVzYnOWZlysB_t2eOQFbm6byLYMiqSYavrJStVQGPinavZ_kPxmJgbccNAF1mgm98ekxXltr8k18ghXkjFbwlhpEutszLhOJC0mkdZxFkKSHm99OFUk",
          "id": "1",
          "rev": "2-cd010b709ed5a0932f23c2ea13c4c5cf"
        },
        {
          "seq": "MGo_tmXV8oawIMsTJta_u1HgVzYnOWZlysB_t2eOQFbm6byLYMiqSYavrJStVQGPinavZ_kPxmJgbccNAF1mgm98ekxXltr8k18ghXkjFbwlhpEutszLhOJC0mkdZxFkKSHm99OFUi",
          "id": "d64b73f378ed9dd1c3f9a4b3485fbbd7",
          "rev": "1-01e7d11edebeb23a9b9df7e2a56f1ad3"
        },
        {
          "seq": "MGo_tmXV8oawIMsTJta_u1HgVzYnOWZlysB_t2eOQFbm6byLYMiqSYavrJStVQGPinavZ_kPxmJgbccNAF1mgm98ekxXltr8k18ghXkjFbwlhpEutszLhOJC0mkdZxFkKSHm99OFUh",
          "id": "_design/_views",
          "rev": "1-f38aa71bff6932ac429407f05553eb78"
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

Note : POST above json document to same blog database.

    curl -X POST localhost:8001/blog -d @./post_view.json -H 'Content-Type: application/json'
    {"_id":"_design/posts","_rev":"1-f776b633e95ef7e9992ad806076025e2"}

Above view definition has necessary information to build and keep the data update to date. Let’s discuss in detail about view definition. Each view in the “views” list has 3 sections. “setup”, “run” and “select”.

“setup”  - set of SQL scripts to create view’s tables and indexes, etc. this scripts runs, whenever a connection open to view’s sqlite3 database.

“run”    - set of SQL scripts keep data update to date. DELETE statement is to delete documents, which are deleted. INSERT OR REPLACE is to insert/replace rows.

“select” - to select data out of view.

View can be executed with followings

    curl -X GET localhost:8001/blog/_design/posts/_all_posts
    {
      "rows": [
        {
          "title": "kdb3 is great",
          "id": "1"
        },
        {
          "title": "getting started",
          "id": "d64b73f378ed9dd1c3f9a4b3485fbbd7"
        }
      ]
    }


## How to Build?

    go build # cgo support required.
    ./kdb3 & # its running at port 8001

## create database

    curl localhost:8001/testdb -X PUT
    {"ok":true}

## delete database

    curl localhost:8001/testdb -X DELETE
    {"ok":true}

## database information

    curl localhost:8001/testdb -X GET
    {"db_name":"testdb","update_seq":"iKgHtVTpywed9yo4O25ZK76l64vYfA86NUvQbVh5aulEGuHGACT6YZte39fZXGsFQR3TdXm3Mn2ZccypZVTnoFzNrnMceWjlQCyLjMaGX_HFL5vDdKRkZDkSpof1ONoT6T7WVr6eZw
","doc_count":1,"deleted_doc_count":0}

## put documents

    curl localhost:8001/testdb -X POST -d '{}'
    {"_id":"1d6707754dfb1133dde2d5eb8f5fbbd7","_rev":"1-99914b932bd37a50b983c5e7c90ae93b"}

    curl localhost:8001/testdb -X POST -d '{"_id":1}'
    {"_id":"1","_rev":"1-99914b932bd37a50b983c5e7c90ae93b"}

    curl localhost:8001/testdb -X POST -d '{"_id":2,"name":"test"}'
    {"_id":"2","_rev":"1-2b895b6efaa28b818284e5c696a18799"}

## update documents

    curl localhost:8001/testdb -X POST -d '{"_id":2, "_rev":"1-2b895b6efaa28b818284e5c696a18799","name":"test1"}'
    {"_id":"2","_rev":"2-07cdac95b6bceaf7857a377fc7695ffb"}

## view documents
    
    curl localhost:8001/testdb/2 -X GET
    {"_id":"2","_rev":"2-07cdac95b6bceaf7857a377fc7695ffb","name":"test1"}

## delete documents

    curl localhost:8001/testdb/2\?rev=2-07cdac95b6bceaf7857a377fc7695ffb -X DELETE
    {"_id":"2","_rev":"3-07cdac95b6bceaf7857a377fc7695ffb","_deleted":true}

## changes 

    curl localhost:8001/testdb/_changes
    {
      "results": [
        {
          "seq": "iKgHtVTpywed9yo4O25ZK76l64vYfA86NUvQbVh5aulEGuHGACT6YZte39fZXGsFQR3TdXm3Mn2ZccypZVTnoFzNrnMceWjlQCyLjMaGX_HFL5vDdKRkZDkSpof1ONoT6T7WVr6e_1",
          "id": "2",
          "rev": "3-07cdac95b6bceaf7857a377fc7695ffb",
          "deleted": true
        },
        {
          "seq": "iKgHtVTpywed9yo4O25ZK76l64vYfA86NUvQbVh5aulEGuHGACT6YZte39fZXGsFQR3TdXm3Mn2ZccypZVTnoFzNrnMceWjlQCyLjMaGX_HFL5vDdKRkZDkSpof1ONoT6T7WVr6eZy",
          "id": "1",
          "rev": "1-99914b932bd37a50b983c5e7c90ae93b"
        },
        {
          "seq": "iKgHtVTpywed9yo4O25ZK76l64vYfA86NUvQbVh5aulEGuHGACT6YZte39fZXGsFQR3TdXm3Mn2ZccypZVTnoFzNrnMceWjlQCyLjMaGX_HFL5vDdKRkZDkSpof1ONoT6T7WVr6eZx",
          "id": "1d6707754dfb1133dde2d5eb8f5fbbd7",
          "rev": "1-99914b932bd37a50b983c5e7c90ae93b"
        },
        {
          "seq": "iKgHtVTpywed9yo4O25ZK76l64vYfA86NUvQbVh5aulEGuHGACT6YZte39fZXGsFQR3TdXm3Mn2ZccypZVTnoFzNrnMceWjlQCyLjMaGX_HFL5vDdKRkZDkSpof1ONoT6T7WVr6eZw",
          "id": "_design/_views",
          "rev": "1-f38aa71bff6932ac429407f05553eb78"
        }
      ]
    }


## incrementally updated materialized View

### to view, view definitions

    curl localhost:8001/testdb/_design/_views -X GET 
    {
      "_id": "_design/_views",
      "_rev": "1-f38aa71bff6932ac429407f05553eb78",
      "views": {
        "_all_docs": {
          "setup": [
            "CREATE TABLE IF NOT EXISTS all_docs (key, value, doc_id,  PRIMARY KEY(key)) WITHOUT ROWID"
          ],
          "run": [
            "DELETE FROM all_docs WHERE doc_id in (SELECT doc_id FROM latest_changes WHERE deleted = 1)",
            "INSERT OR REPLACE INTO all_docs (key, value, doc_id) SELECT doc_id, JSON_OBJECT('rev', rev) as value, doc_id FROM latest_documents WHERE deleted = 0"
          ],
          "select": {
            "default": "SELECT JSON_OBJECT('offset', min(offset),'rows',JSON_GROUP_ARRAY(JSON_OBJECT('key', key, 'value', JSON(value), 'id', doc_id)),'total_rows',(SELECT COUNT(1) FROM all_docs)) FROM (SELECT (ROW_NUMBER() OVER(ORDER BY key) - 1) as offset, * FROM all_docs ORDER BY key) WHERE (${key} IS NULL OR key = ${key}) AND (${next} IS NULL OR key > ${next})",
            "with_docs": "SELECT JSON_OBJECT('offset', min(offset),'rows',JSON_GROUP_ARRAY(JSON_OBJECT('id', doc_id, 'key', key, 'value', JSON(value), 'doc', JSON((SELECT data FROM documents WHERE doc_id = o.doc_id)))),'total_rows',(SELECT COUNT(1) FROM all_docs)) FROM (SELECT (ROW_NUMBER() OVER(ORDER BY key) - 1) as offset, * FROM all_docs ORDER BY key) o WHERE (${key} IS NULL or key = ${key}) AND (${next} IS NULL OR key > ${next})"
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
          "key": "1",
          "value": {
            "rev": "1-99914b932bd37a50b983c5e7c90ae93b"
          },
          "id": "1"
        },
        {
          "key": "1d6707754dfb1133dde2d5eb8f5fbbd7",
          "value": {
            "rev": "1-99914b932bd37a50b983c5e7c90ae93b"
          },
          "id": "1d6707754dfb1133dde2d5eb8f5fbbd7"
        },
        {
          "key": "_design/_views",
          "value": {
            "rev": "1-f38aa71bff6932ac429407f05553eb78"
          },
          "id": "_design/_views"
        }
      ],
      "total_rows": 3
    }
