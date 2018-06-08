# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase

from mongoOperator.helpers.MongoResources import MongoResources


class TestMongoResources(TestCase):
    def test_parseMongoResponse_ok(self):
        with open("tests/fixtures/mongo_responses/replica-status-ok.txt") as f:
            response = MongoResources.parseMongoResponse(f.read())
        expected = {
            "set": "some-db",
            "date": "2018-06-07T09:13:07.663Z",
            "myState": 1,
            "term": 1,
            "heartbeatIntervalMillis": 2000,
            "optimes": {
                "lastCommittedOpTime": {
                    "ts": 1528362783,
                    "t": 1
                },
                "readConcernMajorityOpTime": {
                    "ts": 1528362783,
                    "t": 1
                },
                "appliedOpTime": {
                    "ts": 1528362783,
                    "t": 1
                },
                "durableOpTime": {
                    "ts": 1528362783,
                    "t": 1
                }
            },
            "members": [
                {
                    "_id": 0,
                    "name": "some-db-0.some-db.default.svc.cluster.local:27017",
                    "health": 1,
                    "state": 1,
                    "stateStr": "PRIMARY",
                    "uptime": 210,
                    "optime": {
                        "ts": 1528362783,
                        "t": 1
                    },
                    "optimeDate": "2018-06-07T09:13:03Z",
                    "electionTime": 1528362622,
                    "electionDate": "2018-06-07T09:10:22Z",
                    "configVersion": 1,
                    "self": True
                },
                {
                    "_id": 1,
                    "name": "some-db-1.some-db.default.svc.cluster.local:27017",
                    "health": 1,
                    "state": 2,
                    "stateStr": "SECONDARY",
                    "uptime": 178,
                    "optime": {
                        "ts": 1528362783,
                        "t": 1
                    },
                    "optimeDurable": {
                        "ts": 1528362783,
                        "t": 1
                    },
                    "optimeDate": "2018-06-07T09:13:03Z",
                    "optimeDurableDate": "2018-06-07T09:13:03Z",
                    "lastHeartbeat": "2018-06-07T09:13:07.162Z",
                    "lastHeartbeatRecv": "2018-06-07T09:13:07.265Z",
                    "pingMs": 0,
                    "syncingTo": "some-db-2.some-db.default.svc.cluster.local:27017",
                    "configVersion": 1
                },
                {
                    "_id": 2,
                    "name": "some-db-2.some-db.default.svc.cluster.local:27017",
                    "health": 1,
                    "state": 2,
                    "stateStr": "SECONDARY",
                    "uptime": 178,
                    "optime": {
                        "ts": 1528362783,
                        "t": 1
                    },
                    "optimeDurable": {
                        "ts": 1528362783,
                        "t": 1
                    },
                    "optimeDate": "2018-06-07T09:13:03Z",
                    "optimeDurableDate": "2018-06-07T09:13:03Z",
                    "lastHeartbeat": "2018-06-07T09:13:06.564Z",
                    "lastHeartbeatRecv": "2018-06-07T09:13:06.760Z",
                    "pingMs": 6,
                    "syncingTo": "some-db-0.some-db.default.svc.cluster.local:27017",
                    "configVersion": 1
                }
            ],
            "ok": 1,
            "operationTime": 1528362783,
            "$clusterTime": {
                "clusterTime": 1528362785,
                "signature": {
                    "hash": "AAAAAAAAAAAAAAAAAAAAAAAAAAA=",
                    "keyId": 0
                }
            }
        }
        self.assertEqual(expected, response)

    def test_parseMongoResponse_not_initialized(self):
        with open("tests/fixtures/mongo_responses/replica-status-not-initialized.txt") as f:
            response = MongoResources.parseMongoResponse(f.read())
        expected = {
            "info": "run rs.initiate(...) if not yet done for the set",
            "ok": 0,
            "errmsg": "no replset config has been received",
            "code": 94,
            "codeName": "NotYetInitialized"
        }
        self.assertEqual(expected, response)

    def test_parseMongoResponse_error(self):
        with open("tests/fixtures/mongo_responses/replica-status-error.txt") as f:
            with self.assertRaises(ValueError) as context:
                MongoResources.parseMongoResponse(f.read())
        self.assertEqual("connect failed", str(context.exception))
