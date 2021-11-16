import random

import requests


DBHOST = "http://localhost:8001"
DBNAME = "testdb"



def test_info():
    r = requests.get("{}".format(DBHOST))
    assert r.status_code == 200, "status code should be 200"


def delete_database():
    requests.delete("{}/{}".format(DBHOST, DBNAME))


def test_create_database():
    delete_database()

    r = requests.put("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 201

    r = requests.get("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 200
    rs = r.json()
    assert rs["doc_count"] == 1
    assert rs["deleted_doc_count"] == 0
    assert rs["name"] == DBNAME
    assert len(rs["update_seq"]) == 138

    r = requests.get("{}/_cat/dbs".format(DBHOST))
    assert DBNAME in r.json(), "Failed: get created database"

    r = requests.get("{}/{}/_design/_views".format(DBHOST, DBNAME))
    assert r.status_code == 200

    delete_database()


def test_create_database_with_invalid_name():
    DBNAME = "$3213324"
    r = requests.put("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 400, "Failed: expecting bad request"

    r = requests.get("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 404, "Failed: expecting not found"

    r = requests.get("{}/_cat/dbs".format(DBHOST))
    assert DBNAME not in r.json(), "Failed: expecting not found"


def test_create_database_exists():
    delete_database()

    r = requests.put("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 201, "Failed: create database"

    r = requests.put("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 412, "Failed: expecting database already exists"

    delete_database()


def test_delete_database():
    delete_database()

    r = requests.put("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 201, "Failed: create database"

    r = requests.delete("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 200, "Failed: delete database"

    r = requests.get("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 404, "Failed: delete database"

    r = requests.get("{}/_cat/dbs".format(DBHOST))
    assert DBNAME not in r.json(), "Failed: expecting not found"

    delete_database()


def test_single_insert_documents():
    delete_database()

    seed_data = []
    for x in range(12):
        x = x + 1
        if x < 6:
            seed_data.append({"_id": x, "foo": "bar"})
        else:
            seed_data.append({"foo": "bar"})

    r = requests.put("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 201

    for seed in seed_data:
        r = requests.post("{}/{}".format(DBHOST, DBNAME), json=seed, headers={"Content-Type": "application/json"})
        assert r.status_code == 200

        rs = r.json()
        assert "_id" in rs
        assert "_rev" in rs

        seed["_id"] = rs["_id"]
        seed["_rev"] = rs["_rev"]

    for seed in seed_data:
        r = requests.get("{}/{}/{}".format(DBHOST, DBNAME, seed["_id"]), headers={"Content-Type": "application/json"})
        assert r.status_code == 200
        assert r.json()["_rev"][:2] == "1-"

    r = requests.get("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 200
    rs = r.json()
    assert rs["doc_count"] == 13
    assert rs["deleted_doc_count"] == 0

    delete_database()


def test_single_insert_invalid_documents():
    delete_database()

    r = requests.put("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 201

    r = requests.post("{}/{}".format(DBHOST, DBNAME), json={}, headers={"Content-Type": "application/json"})
    assert r.status_code == 200

    _id = r.json()["_id"]

    r = requests.put("{}/{}/{}".format(DBHOST, DBNAME, _id), json=[], headers={"Content-Type": "application/json"})
    assert r.status_code == 400

    r = requests.post("{}/{}".format(DBHOST, DBNAME), json=[], headers={"Content-Type": "application/json"})
    assert r.status_code == 400

    r = requests.post("{}/{}".format(DBHOST, DBNAME), json={"_rev":"1-dfasdfsfsdfsdfasdfasfdsadfsdf"},
                      headers={"Content-Type": "application/json"})
    assert r.status_code == 400

    r = requests.put("{}/{}/{}".format(DBHOST, DBNAME, _id), json={"_rev": "1-dfasdfsfsdfsdfasdfasfdsadfsdf"}, headers={"Content-Type": "application/json"})
    assert r.status_code == 400

    r = requests.post("{}/{}".format(DBHOST, DBNAME), json={"_deleted": True},
                      headers={"Content-Type": "application/json"})
    assert r.status_code == 400

    r = requests.put("{}/{}/{}".format(DBHOST, DBNAME, _id), json={"deleted": True}, headers={"Content-Type": "application/json"})
    assert r.status_code == 409

    delete_database()


def test_conflict_single_insert_update_delete_documents():
    delete_database()

    seed = {"foo": "bar"}

    r = requests.put("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 201

    r = requests.post("{}/{}".format(DBHOST, DBNAME), json=seed, headers={"Content-Type": "application/json"})
    rs = r.json()
    assert r.status_code == 200
    assert "_id" in rs
    assert "_rev" in rs
    assert rs["_rev"][:2] == "1-"

    _id = rs["_id"]
    _rev1 = rs["_rev"]

    seed["_id"] = _id

    r = requests.post("{}/{}".format(DBHOST, DBNAME), json=seed, headers={"Content-Type": "application/json"})
    assert r.status_code == 409

    r = requests.put("{}/{}/{}".format(DBHOST, DBNAME, _id), json=seed, headers={"Content-Type": "application/json"})
    assert r.status_code == 409

    seed["_rev"] = _rev1

    r = requests.post("{}/{}".format(DBHOST, DBNAME), json=seed, headers={"Content-Type": "application/json"})
    assert r.status_code == 200
    rs = r.json()
    assert rs["_rev"][:2] == "2-"

    _rev2 = rs["_rev"]
    seed["_rev"] = _rev2

    r = requests.put("{}/{}/{}".format(DBHOST, DBNAME, _id), json=seed, headers={"Content-Type": "application/json"})
    assert r.status_code == 200
    rs = r.json()
    assert rs["_rev"][:2] == "3-"

    _rev3 = rs["_rev"]
    seed["_rev"] = _rev3

    r = requests.delete("{}/{}/{}?rev={}".format(DBHOST, DBNAME, _id, _rev2))
    assert r.status_code == 409

    r = requests.delete("{}/{}/{}?rev={}".format(DBHOST, DBNAME, _id, _rev3))
    assert r.status_code == 200

    r = requests.delete("{}/{}/{}?rev={}".format(DBHOST, DBNAME, _id, _rev3))
    assert r.status_code == 409

    #delete and insert
    r = requests.post("{}/{}".format(DBHOST, DBNAME), json={"_id": _id})
    assert r.status_code == 200
    assert r.json()['_rev'][:2] == "5-"

    delete_database()


def test_single_update_documents():
    delete_database()

    seed_data = []
    for x in range(12):
        x = x + 1
        if x < 6:
            seed_data.append({"_id": x, "foo": "bar"})
        else:
            seed_data.append({"foo": "bar"})

    r = requests.put("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 201

    # creating
    for seed in seed_data:
        r = requests.post("{}/{}".format(DBHOST, DBNAME), json=seed, headers={"Content-Type": "application/json"})
        assert r.status_code == 200

        rs = r.json()
        assert "_id" in rs
        assert "_rev" in rs

        seed["_id"] = rs["_id"]
        seed["_rev"] = rs["_rev"]

    # get
    for seed in seed_data:
        r = requests.get("{}/{}/{}".format(DBHOST, DBNAME, seed["_id"]), headers={"Content-Type": "application/json"})
        rs = r.json()
        assert r.status_code == 200
        assert rs["_rev"][0] == "1", "failed, Expecting version number 1"

    # update
    for seed in seed_data:
        r = requests.post("{}/{}".format(DBHOST, DBNAME), headers={"Content-Type": "application/json"}, json=seed)
        assert r.status_code == 200
        rs = r.json()
        assert "_id" in rs
        assert "_rev" in rs
        seed["_id"] = rs["_id"]
        seed["_rev"] = rs["_rev"]

    # get
    for seed in seed_data:
        r = requests.get("{}/{}/{}".format(DBHOST, DBNAME, seed["_id"]), headers={"Content-Type": "application/json"})
        rs = r.json()
        assert r.status_code == 200
        assert rs["_rev"][0] == "2", "failed, Expecting version number 2"

    # update
    for seed in seed_data:
        r = requests.put("{}/{}/{}".format(DBHOST, DBNAME, seed["_id"]), headers={"Content-Type": "application/json"}, json=seed)
        assert r.status_code == 200

    # get
    for seed in seed_data:
        r = requests.get("{}/{}/{}".format(DBHOST, DBNAME, seed["_id"]),
                         headers={"Content-Type": "application/json"})
        rs = r.json()
        assert r.status_code == 200
        assert rs["_rev"][0] == "3", "failed, Expecting version number 2"

    delete_database()


def test_single_delete_documents():
    delete_database()

    seed_data = []
    for x in range(12):
        x = x + 1
        if x < 6:
            seed_data.append({"_id": x, "foo": "bar"})
        else:
            seed_data.append({"foo": "bar"})

    r = requests.put("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 201

    for seed in seed_data:
        r = requests.post("{}/{}".format(DBHOST, DBNAME), json=seed, headers={"Content-Type": "application/json"})
        assert r.status_code == 200

        rs = r.json()
        assert "_id" in rs
        assert "_rev" in rs

        seed["_id"] = rs["_id"]
        seed["_rev"] = rs["_rev"]

    for seed in seed_data[:6]:
        r = requests.delete("{}/{}/{}?rev={}".format(DBHOST, DBNAME, seed["_id"], seed["_rev"]), headers={"Content-Type": "application/json"})
        assert r.status_code == 200

        rs = r.json()
        assert "_id" in rs
        assert "_rev" in rs
        seed["_id"] = rs["_id"]
        seed["_rev"] = rs["_rev"]

    seed = seed_data[6]
    seed["_deleted"] = True
    r = requests.post("{}/{}".format(DBHOST, DBNAME), json=seed, headers={"Content-Type": "application/json"})
    assert r.status_code == 200

    for seed in seed_data[:7]:
        r = requests.get("{}/{}/{}".format(DBHOST, DBNAME, seed["_id"]), headers={"Content-Type": "application/json"})
        assert r.status_code == 404

    r = requests.get("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 200
    rs = r.json()
    assert rs["doc_count"] == 6
    assert rs["deleted_doc_count"] == 7

    delete_database()


def test_bulk_insert_documents():
    delete_database()

    seed_data = []
    for x in range(12):
        x = x + 1
        if x < 6:
            seed_data.append({"_id": x, "foo": "bar"})
        else:
            seed_data.append({"foo": "bar"})

    r = requests.put("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 201

    r = requests.post("{}/{}/_bulk_docs".format(DBHOST, DBNAME), json={"_docs": seed_data}, headers={"Content-Type": "application/json"})
    assert r.status_code == 200

    rs = r.json()
    for i in range(len(seed_data)):
        seed_data[i]["_id"] = rs[i]["_id"]
        seed_data[i]["_rev"] = rs[i]["_rev"]

    r = requests.get("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 200
    rs = r.json()
    assert rs["doc_count"] == 13
    assert rs["deleted_doc_count"] == 0

    r = requests.post("{}/{}/_bulk_docs".format(DBHOST, DBNAME), json=[],
                      headers={"Content-Type": "application/json"})
    assert r.status_code == 400

    r = requests.post("{}/{}/_bulk_docs".format(DBHOST, DBNAME), json={},
                      headers={"Content-Type": "application/json"})
    assert r.status_code == 400

    r = requests.post("{}/{}/_bulk_docs".format(DBHOST, DBNAME), json={"_docs":[]},
                      headers={"Content-Type": "application/json"})
    assert r.status_code == 400

    seed = seed_data.pop()
    del seed["_rev"]
    r = requests.post("{}/{}/_bulk_docs".format(DBHOST, DBNAME), json={"_docs":[{}, {"_id": "with_id"}, {"_rev": "1"}, seed , seed_data.pop(), seed_data.pop()]},
                      headers={"Content-Type": "application/json"})
    rs = r.json()
    assert r.status_code == 200
    assert len(rs) == 6

    assert rs[0]["_rev"][0] == "1"
    assert rs[1]["_rev"][0] == "1"
    assert rs[4]["_rev"][0] == "2"
    assert rs[5]["_rev"][0] == "2"

    assert rs[1]["_id"] == "with_id"
    assert rs[2]["error"] == "invalid_rev_id"
    assert rs[3]["error"] == "doc_conflict"

    delete_database()


def test_bulk_get_documents():
    delete_database()

    seed_data = []
    for x in range(12):
        x = x + 1
        if x < 6:
            seed_data.append({"_id": x, "foo": "bar"})
        else:
            seed_data.append({"foo": "bar"})

    r = requests.put("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 201

    for seed in seed_data:
        r = requests.post("{}/{}".format(DBHOST, DBNAME), json=seed, headers={"Content-Type": "application/json"})
        assert r.status_code == 200

        rs = r.json()
        assert "_id" in rs
        assert "_rev" in rs
        seed["_id"] = rs["_id"]
        seed["_rev"] = rs["_rev"]

    r = requests.get("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 200
    rs = r.json()
    assert rs["doc_count"] == 13
    assert rs["deleted_doc_count"] == 0

    req_data = {"_docs": [{"_id": x["_id"]} for x in seed_data] }

    r = requests.post("{}/{}/_bulk_gets".format(DBHOST, DBNAME), json=req_data,
                      headers={"Content-Type": "application/json"})
    assert r.status_code == 200
    assert len(r.json()) == 12

    r = requests.post("{}/{}/_bulk_gets".format(DBHOST, DBNAME), json=[],
                      headers={"Content-Type": "application/json"})
    assert r.status_code == 400

    r = requests.post("{}/{}/_bulk_gets".format(DBHOST, DBNAME), json={},
                      headers={"Content-Type": "application/json"})
    assert r.status_code == 400
    r = requests.post("{}/{}/_bulk_gets".format(DBHOST, DBNAME), json={"_docs":[]},
                      headers={"Content-Type": "application/json"})
    assert r.status_code == 400

    req_data_1 = [req_data["_docs"].pop(), req_data["_docs"].pop(), {"_id": "4234"}]
    item = req_data["_docs"].pop()
    item["_rev"] = "1-34234234"
    req_data_1.append(item)

    item = req_data["_docs"].pop()
    item["_rev"] = "1-12345678123456781234567812345678"
    req_data_1.append(item)

    r = requests.post("{}/{}/_bulk_gets".format(DBHOST, DBNAME), json={"_docs": req_data_1},
                      headers={"Content-Type": "application/json"})
    assert r.status_code == 200
    rs = r.json()

    assert len(rs) == 5
    assert rs[0]["_rev"][:2] == "1-"
    assert rs[1]["_rev"][:2] == "1-"
    assert rs[2]["error"] == "doc_not_found"
    assert rs[3]["error"] == "invalid_rev_id"
    assert rs[4]["error"] == "doc_not_found"
    delete_database()


def test_get_all_docs():
    delete_database()

    seed_data = []
    for x in range(12):
        x = x + 1
        if x < 6:
            seed_data.append({"_id": x, "foo": "bar"})
        else:
            seed_data.append({"foo": "bar"})

    r = requests.put("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 201

    for seed in seed_data:
        r = requests.post("{}/{}".format(DBHOST, DBNAME), json=seed, headers={"Content-Type": "application/json"})
        assert r.status_code == 200

        rs = r.json()
        assert "_id" in rs
        assert "_rev" in rs
        seed["_id"] = rs["_id"]
        seed["_rev"] = rs["_rev"]

    r = requests.get("{}/{}".format(DBHOST, DBNAME))
    assert r.status_code == 200
    rs = r.json()
    assert rs["doc_count"] == 13
    assert rs["deleted_doc_count"] == 0

    r = requests.get("{}/{}/_all_docs".format(DBHOST, DBNAME), json=seed, headers={"Content-Type": "application/json"})
    assert r.status_code == 200
    rs = r.json()
    assert len(rs["rows"]) == 10
    assert rs["total_rows"] == 13
    assert rs["offset"] == 1

    r = requests.get("{}/{}/_all_docs?page=1".format(DBHOST, DBNAME), json=seed, headers={"Content-Type": "application/json"})
    assert r.status_code == 200
    rs = r.json()
    assert len(rs["rows"]) == 10
    assert rs["total_rows"] == 13
    assert rs["offset"] == 1

    r = requests.get("{}/{}/_all_docs?page=2".format(DBHOST, DBNAME), json=seed,
                     headers={"Content-Type": "application/json"})
    assert r.status_code == 200
    rs = r.json()
    assert len(rs["rows"]) == 3
    assert rs["total_rows"] == 13
    assert rs["offset"] == 11

    r = requests.get("{}/{}/_all_docs?page=1&limit=13".format(DBHOST, DBNAME), json=seed,
                     headers={"Content-Type": "application/json"})
    assert r.status_code == 200
    rs = r.json()
    assert len(rs["rows"]) == 13
    assert rs["total_rows"] == 13
    assert rs["offset"] == 1

    r = requests.get("{}/{}/_all_docs?limit=13".format(DBHOST, DBNAME), json=seed,
                     headers={"Content-Type": "application/json"})
    assert r.status_code == 200
    rs = r.json()
    assert len(rs["rows"]) == 13
    assert rs["total_rows"] == 13
    assert rs["offset"] == 1

    items = {item["id"]: item for item in rs["rows"]}
    for seed in seed_data:
        assert seed["_id"] in items
    assert "_design/_views" in items

    r = requests.post("{}/{}".format(DBHOST, DBNAME), json={}, headers={"Content-Type": "application/json"})
    assert r.status_code == 200

    r = requests.get("{}/{}/_all_docs?limit=13".format(DBHOST, DBNAME), json=seed,
                     headers={"Content-Type": "application/json"})
    assert r.status_code == 200
    rs = r.json()
    assert len(rs["rows"]) == 13
    assert rs["total_rows"] == 14
    assert rs["offset"] == 1

    delete_database()


def test_curd_design_docs():
    delete_database()

    r1 = requests.put("{}/{}".format(DBHOST, DBNAME))
    assert r1.status_code == 201

    r2 = requests.get("{}/{}/_design/_views".format(DBHOST, DBNAME))
    assert r2.status_code == 200

    r3 = requests.put("{}/{}/_design/_views".format(DBHOST, DBNAME), json=r2.json(),
                      headers={"Content-Type": "application/json"})
    assert r3.status_code == 200
    assert r3.json()["_rev"][:2] == "2-"

    r3 = requests.post("{}/{}".format(DBHOST, DBNAME), json=r3.json(),
                      headers={"Content-Type": "application/json"})
    assert r3.status_code == 200
    assert r3.json()["_rev"][:2] == "3-"

    delete_database()

# design docs CURD
# changes api