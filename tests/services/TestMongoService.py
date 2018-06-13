# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from base64 import b64encode

from kubernetes.client import V1Secret, V1ObjectMeta
from kubernetes.client.rest import ApiException
from typing import Union
from unittest import TestCase
from unittest.mock import MagicMock, patch, call

from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService
from mongoOperator.services.MongoService import MongoService
from tests.test_utils import getExampleClusterDefinition


@patch("mongoOperator.services.MongoService.sleep", MagicMock())
class TestMongoService(TestCase):
    maxDiff = None

    def setUp(self):
        super().setUp()
        self.kubernetes_service: Union[MagicMock, KubernetesService] = MagicMock()
        self.kubernetes_service.getSecret.return_value = V1Secret(
            metadata=V1ObjectMeta(name="mongo-cluster-admin-credentials", namespace="default"),
            data={"password": b64encode(b"random-password"), "username": b64encode(b"root")},
        )

        self.service = MongoService(self.kubernetes_service)
        self.cluster_dict = getExampleClusterDefinition()
        self.cluster_object = V1MongoClusterConfiguration(**self.cluster_dict)

        self.not_initialized_response = {
            "info": "run rs.initiate(...) if not yet done for the set",
            "ok": 0,
            "errmsg": "no replset config has been received",
            "code": 94,
            "codeName": "NotYetInitialized"
        }

        self.initiate_ok_response = {
            "ok": 1,
            "operationTime": 1528365094.1,
            "$clusterTime": {
                "clusterTime": 1528365094.1,
                "signature": {
                    "hash": "AAAAAAAAAAAAAAAAAAAAAAAAAAA=",
                    "keyId": 0
                }
            }
        }

        self.initiate_not_found_response = {
            "ok": 0,
            "errmsg": "replSetInitiate quorum check failed because not all proposed set members responded "
                      "affirmatively: some-db-2.some-db.default.svc.cluster.local:27017 failed with Connection refused",
            "code": 74,
            "codeName": "NodeNotFound"
        }

        self.expected_cluster_config = json.dumps({
            "_id": "mongo-cluster",
            "version": 1,
            "members": [
                {"_id": 0, "host": "mongo-cluster-0.mongo-cluster.default.svc.cluster.local"},
                {"_id": 1, "host": "mongo-cluster-1.mongo-cluster.default.svc.cluster.local"},
                {"_id": 2, "host": "mongo-cluster-2.mongo-cluster.default.svc.cluster.local"}
            ]
        })

        self.expected_user_create = """
            admin = db.getSiblingDB("admin")
            admin.createUser({
                user: "root", pwd: "random-password",
                roles: [ { role: "root", db: "admin" } ]
            })
            admin.auth("root", "random-password")
        """

    def _getFixture(self, name):
        with open("tests/fixtures/mongo_responses/{}.txt".format(name)) as f:
            return f.read()

    def test__execInPod(self):
        self.kubernetes_service.execInPod.return_value = self._getFixture("replica-status-not-initialized")
        result = self.service._execInPod(0, "cluster", "default", "rs.status()")
        self.assertEquals(self.not_initialized_response, result)
        expected_calls = [call.execInPod(
            'mongodb', 'cluster-0', 'default', ['mongo', 'localhost:27017/admin', '--eval', 'rs.status()']
        )]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)

    def test__execInPod_NodeNotFound(self):
        self.kubernetes_service.execInPod.side_effect = (self._getFixture("initiate-not-found"),
                                                         self._getFixture("initiate-not-found"),
                                                         self._getFixture("initiate-ok"))
        result = self.service._execInPod(1, "cluster", "default", "rs.initiate({})")
        self.assertEquals(self.initiate_ok_response, result)
        expected_calls = 3 * [call.execInPod(
            'mongodb', 'cluster-1', 'default', ['mongo', 'localhost:27017/admin', '--eval', 'rs.initiate({})']
        )]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)

    def test__execInPod_connect_failed(self):
        self.kubernetes_service.execInPod.side_effect = ValueError("connect failed"), self._getFixture("initiate-ok")
        result = self.service._execInPod(1, "cluster", "default", "rs.test()")
        self.assertEquals(self.initiate_ok_response, result)
        expected_calls = 2 * [call.execInPod(
            'mongodb', 'cluster-1', 'default', ['mongo', 'localhost:27017/admin', '--eval', 'rs.test()']
        )]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)

    def test__execInPod_handshake_status(self):
        self.kubernetes_service.execInPod.side_effect = (ApiException(500, reason="Handshake status: Failed!"),
                                                         self._getFixture("initiate-ok"))
        result = self.service._execInPod(1, "cluster", "default", "rs.test()")
        self.assertEquals(self.initiate_ok_response, result)
        expected_calls = 2 * [call.execInPod(
            'mongodb', 'cluster-1', 'default', ['mongo', 'localhost:27017/admin', '--eval', 'rs.test()']
        )]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)

    def test__execInPod_ValueError(self):
        self.kubernetes_service.execInPod.side_effect = ValueError("Value error.")
        with self.assertRaises(ValueError) as context:
            self.service._execInPod(1, "cluster", "default", "rs.test()")
        self.assertEquals("Value error.", str(context.exception))
        expected_calls = [call.execInPod(
            'mongodb', 'cluster-1', 'default', ['mongo', 'localhost:27017/admin', '--eval', 'rs.test()']
        )]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)

    def test__execInPod_ApiException(self):
        self.kubernetes_service.execInPod.side_effect = ApiException(400, reason="A reason.")
        with self.assertRaises(ApiException) as context:
            self.service._execInPod(5, "mongo-cluster", "ns", "rs.test()")

        self.assertEquals("(400)\nReason: A reason.\n", str(context.exception))
        expected_calls = [call.execInPod(
            'mongodb', 'mongo-cluster-5', 'ns', ['mongo', 'localhost:27017/admin', '--eval', 'rs.test()']
        )]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)

    def test__execInPod_TimeoutError(self):
        self.kubernetes_service.execInPod.side_effect = (ValueError("connection attempt failed"),
                                                         ApiException(500, reason="Handshake status: Failed!"),
                                                         self._getFixture("initiate-not-found"),
                                                         ApiException(404, reason="Handshake status: error"))
        with self.assertRaises(TimeoutError) as context:
            self.service._execInPod(5, "mongo-cluster", "ns", "rs.test()")

        self.assertEquals("Could not check the replica set after 4 retries!", str(context.exception))
        expected_calls = 4 * [call.execInPod(
            'mongodb', 'mongo-cluster-5', 'ns', ['mongo', 'localhost:27017/admin', '--eval', 'rs.test()']
        )]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)

    def test_initializeReplicaSet(self):
        self.kubernetes_service.execInPod.return_value = self._getFixture("initiate-ok")
        self.service.initializeReplicaSet(self.cluster_object)
        expected_calls = [call.execInPod(
            'mongodb', 'mongo-cluster-0', 'default', [
                'mongo', 'localhost:27017/admin', '--eval', 'rs.initiate({})'.format(self.expected_cluster_config)
            ]
        )]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)

    def test_initializeReplicaSet_ValueError(self):
        exec_result = self._getFixture("initiate-not-found").replace("NodeNotFound", "Error")
        self.kubernetes_service.execInPod.return_value = exec_result
        with self.assertRaises(ValueError) as context:
            self.service.initializeReplicaSet(self.cluster_object)

        self.initiate_not_found_response["codeName"] = "Error"
        self.assertEquals("Unexpected response initializing replica set mongo-cluster @ ns/default:\n" +
                          str(self.initiate_not_found_response),
                          str(context.exception))

    def test_reconfigureReplicaSet(self):
        self.kubernetes_service.execInPod.return_value = self._getFixture("initiate-ok")
        self.service.reconfigureReplicaSet(self.cluster_object)
        expected_calls = [call.execInPod(
            'mongodb', 'mongo-cluster-0', 'default', [
                'mongo', 'localhost:27017/admin', '--eval', 'rs.reconfig({})'.format(self.expected_cluster_config)
            ]
        )]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)

    def test_reconfigureReplicaSet_ValueError(self):
        exec_result = self._getFixture("initiate-not-found").replace("NodeNotFound", "Error")
        self.kubernetes_service.execInPod.return_value = exec_result
        with self.assertRaises(ValueError) as context:
            self.service.reconfigureReplicaSet(self.cluster_object)

        self.initiate_not_found_response["codeName"] = "Error"
        self.assertEquals("Unexpected response reconfiguring replica set mongo-cluster @ ns/default:\n" +
                          str(self.initiate_not_found_response),
                          str(context.exception))

    def test_checkReplicaSetOrInitialize_ok(self):
        self.kubernetes_service.execInPod.return_value = self._getFixture("replica-status-ok")
        self.service.checkReplicaSetOrInitialize(self.cluster_object)
        expected_calls = [call.execInPod('mongodb', 'mongo-cluster-0', 'default',
                                         ['mongo', 'localhost:27017/admin', '--eval', 'rs.status()'])]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)

    def test_checkReplicaSetOrInitialize_initialize(self):
        self.kubernetes_service.execInPod.side_effect = (self._getFixture("replica-status-not-initialized"),
                                                         self._getFixture("initiate-ok"))
        self.service.checkReplicaSetOrInitialize(self.cluster_object)
        expected_calls = [call.execInPod(
            'mongodb', 'mongo-cluster-0', 'default', ['mongo', 'localhost:27017/admin', '--eval', 'rs.status()']
        ), call.execInPod(
            'mongodb', 'mongo-cluster-0', 'default',
            ['mongo', 'localhost:27017/admin', '--eval', 'rs.initiate({})'.format(self.expected_cluster_config)]
        )]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)

    def test_checkReplicaSetOrInitialize_reconfigure(self):
        self.cluster_object.spec.mongodb.replicas = 4
        self.kubernetes_service.execInPod.return_value = self._getFixture("replica-status-ok")
        self.service.checkReplicaSetOrInitialize(self.cluster_object)

        cluster_config = json.loads(self.expected_cluster_config)
        cluster_config["members"].append({"_id": 3, "host": "mongo-cluster-3.mongo-cluster.default.svc.cluster.local"})
        self.expected_cluster_config = json.dumps(cluster_config)

        expected_calls = [call.execInPod(
            'mongodb', 'mongo-cluster-0', 'default', ['mongo', 'localhost:27017/admin', '--eval', 'rs.status()']
        ), call.execInPod(
            'mongodb', 'mongo-cluster-0', 'default',
            ['mongo', 'localhost:27017/admin', '--eval', 'rs.reconfig({})'.format(self.expected_cluster_config)],
        )]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)

    def test_checkReplicaSetOrInitialize_ValueError(self):
        response = self._getFixture("replica-status-ok").replace('"ok" : 1', '"ok" : 2')
        self.kubernetes_service.execInPod.return_value = response

        with self.assertRaises(ValueError) as context:
            self.service.checkReplicaSetOrInitialize(self.cluster_object)

        expected_calls = [call.execInPod(
            'mongodb', 'mongo-cluster-0', 'default', ['mongo', 'localhost:27017/admin', '--eval', 'rs.status()']
        )]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)
        self.assertIn("Unexpected response trying to check replicas: ", str(context.exception))

    def test_createUsers_ok(self):
        self.kubernetes_service.execInPod.return_value = self._getFixture("createUser-ok")

        self.service.createUsers(self.cluster_object)
        expected_calls = [
            call.getSecret('mongo-cluster-admin-credentials', 'default'),
            call.execInPod('mongodb', 'mongo-cluster-0', 'default',
                           ['mongo', 'localhost:27017/admin', '--eval', self.expected_user_create])
        ]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)

    def test_createUsers_ValueError(self):
        self.kubernetes_service.execInPod.return_value = self._getFixture("createUser-ok").replace('"user"', '"error"')

        with self.assertRaises(ValueError) as context:
            self.service.createUsers(self.cluster_object)
        expected_calls = [
            call.getSecret('mongo-cluster-admin-credentials', 'default'),
            call.execInPod('mongodb', 'mongo-cluster-0', 'default',
                           ['mongo', 'localhost:27017/admin', '--eval', self.expected_user_create])
        ]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)
        self.assertEquals("Unexpected response creating users for pod mongo-cluster-0 @ ns/default:\n"
                          "{'error': 'root', 'roles': [{'role': 'root', 'db': 'admin'}]}", str(context.exception))

    def test_createUsers_not_master_then_already_exists(self):
        self.kubernetes_service.execInPod.side_effect = (self._getFixture("createUser-notMaster"),
                                                         self._getFixture("createUser-exists"))

        self.service.createUsers(self.cluster_object)
        expected_calls = [
            call.getSecret('mongo-cluster-admin-credentials', 'default'),
            call.execInPod('mongodb', 'mongo-cluster-0', 'default',
                           ['mongo', 'localhost:27017/admin', '--eval', self.expected_user_create]),
            call.execInPod('mongodb', 'mongo-cluster-1', 'default',
                           ['mongo', 'localhost:27017/admin', '--eval', self.expected_user_create]),
        ]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)

    def test_createUsers_TimeoutError(self):
        self.kubernetes_service.execInPod.return_value = self._getFixture("createUser-notMaster")

        with self.assertRaises(TimeoutError) as context:
            self.service.createUsers(self.cluster_object)
        expected_calls = [call.getSecret('mongo-cluster-admin-credentials', 'default')] + [
            call.execInPod('mongodb', 'mongo-cluster-' + str(pod), 'default',
                           ['mongo', 'localhost:27017/admin', '--eval', self.expected_user_create])
            for _ in range(4) for pod in range(3)
        ]
        self.assertEquals(expected_calls, self.kubernetes_service.mock_calls)
        self.assertEquals("Could not create users in any of the 3 pods of cluster mongo-cluster @ ns/default.",
                          str(context.exception))
