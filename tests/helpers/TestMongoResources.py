# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase

from mongoOperator.helpers.MongoResources import MongoResources


class TestMongoResources(TestCase):
    # note: most methods are tested in TestMongoService.py

    def test_parseMongoResponse_ok(self):
        with open("tests/fixtures/mongo_responses/replica-status-ok.txt") as f:
            response = MongoResources.parseMongoResponse(f.read())
        expected = {
            '$clusterTime': {
                'clusterTime': 1528362785.0,
                'signature': {
                    'hash': 'AAAAAAAAAAAAAAAAAAAAAAAAAAA=',
                    'keyId': 0
                }
            },
            'date': '2018-06-07T09:13:07.663Z',
            'heartbeatIntervalMillis': 2000,
            'members': [{
                '_id': 0,
                'configVersion': 1,
                'electionDate': '2018-06-07T09:10:22Z',
                'electionTime': 1528362622.1,
                'health': 1,
                'name': 'some-db-0.some-db.default.svc.cluster.local:27017',
                'optime': {'t': 1, 'ts': 1528362783.1},
                'optimeDate': '2018-06-07T09:13:03Z',
                'self': True,
                'state': 1,
                'stateStr': 'PRIMARY',
                'uptime': 210
            }, {
                '_id': 1,
                'configVersion': 1,
                'health': 1,
                'lastHeartbeat': '2018-06-07T09:13:07.162Z',
                'lastHeartbeatRecv': '2018-06-07T09:13:07.265Z',
                'name': 'some-db-1.some-db.default.svc.cluster.local:27017',
                'optime': {'t': 1, 'ts': 1528362783.1},
                'optimeDate': '2018-06-07T09:13:03Z',
                'optimeDurable': {'t': 1, 'ts': 1528362783.1},
                'optimeDurableDate': '2018-06-07T09:13:03Z',
                'pingMs': 0,
                'state': 2,
                'stateStr': 'SECONDARY',
                'syncingTo': 'some-db-2.some-db.default.svc.cluster.local:27017',
                'uptime': 178
            }, {
                '_id': 2,
                'configVersion': 1,
                'health': 1,
                'lastHeartbeat': '2018-06-07T09:13:06.564Z',
                'lastHeartbeatRecv': '2018-06-07T09:13:06.760Z',
                'name': 'some-db-2.some-db.default.svc.cluster.local:27017',
                'optime': {'t': 1, 'ts': 1528362783.1},
                'optimeDate': '2018-06-07T09:13:03Z',
                'optimeDurable': {'t': 1, 'ts': 1528362783.1},
                'optimeDurableDate': '2018-06-07T09:13:03Z',
                'pingMs': 6,
                'state': 2,
                'stateStr': 'SECONDARY',
                'syncingTo': 'some-db-0.some-db.default.svc.cluster.local:27017',
                'uptime': 178
            }],
            'myState': 1,
            'ok': 1,
            'operationTime': 1528362783.1,
            'optimes': {
                'appliedOpTime': {'t': 1, 'ts': 1528362783.1},
                'durableOpTime': {'t': 1, 'ts': 1528362783.1},
                'lastCommittedOpTime': {'t': 1, 'ts': 1528362783.1},
                'readConcernMajorityOpTime': {'t': 1, 'ts': 1528362783.1}
            },
            'set': 'some-db',
            'term': 1
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

    def test_parseMongoResponse_empty(self):
        self.assertEqual({}, MongoResources.parseMongoResponse(''))

    def test_parseMongoResponse_only_version(self):
        self.assertEqual({}, MongoResources.parseMongoResponse("MongoDB shell version v3.6.4\n"))

    def test_parseMongoResponse_version_twice(self):
        self.assertEqual({}, MongoResources.parseMongoResponse(
            "MongoDB shell version v3.6.4\n"
            "connecting to: mongodb://localhost:27017/admin\n"
            "MongoDB server version: 3.6.4\n"
        ))

    def test_parseMongoResponse_bad_json(self):
        with open("tests/fixtures/mongo_responses/replica-status-ok.txt") as f:
            with self.assertRaises(ValueError) as context:
                MongoResources.parseMongoResponse(f.read().replace("Timestamp", "TimeStamp"))
        self.assertIn("Cannot parse JSON because of error", str(context.exception))

    def test_parseMongoResponse_user_created(self):
        with open("tests/fixtures/mongo_responses/createUser-ok.txt") as f:
            response = MongoResources.parseMongoResponse(f.read())
        expected = {"user": "root", "roles": [{"role": "root", "db": "admin"}]}
        self.assertEqual(expected, response)
