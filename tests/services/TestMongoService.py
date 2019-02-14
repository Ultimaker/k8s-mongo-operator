# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from base64 import b64encode

from kubernetes.client import V1Secret, V1ObjectMeta
from typing import Union
from unittest import TestCase
from unittest.mock import MagicMock, patch, Mock

from mongoOperator.helpers.MongoResources import MongoResources
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService
from mongoOperator.services.MongoService import MongoService
from tests.test_utils import getExampleClusterDefinition
from bson.json_util import loads
from pymongo.errors import OperationFailure, ConnectionFailure


@patch("mongoOperator.services.MongoService.sleep", MagicMock())
@patch("mongoOperator.services.MongoService.MongoClient")
class TestMongoService(TestCase):
    maxDiff = None

    def setUp(self):
        super().setUp()
        self.kubernetes_service: Union[MagicMock, KubernetesService] = MagicMock()
        self.dummy_credentials = b64encode(json.dumps({"user": "password"}).encode())
        self.kubernetes_service.getSecret.return_value = V1Secret(
            metadata=V1ObjectMeta(name="mongo-cluster-admin-credentials", namespace="default"),
            data={
                "password": b64encode(b"random-password"),
                "username": b64encode(b"root"),
                "json": self.dummy_credentials
            },
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

        self.initiate_ok_response = loads("""
            {"ok": 1.0, "operationTime": {"$timestamp": {"t": 1549963040, "i": 1}}, "$clusterTime": {"clusterTime":
            {"$timestamp": {"t": 1549963040, "i": 1}}, "signature": {"hash": {"$binary": "AAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "$type": "00"}, "keyId": 0}}}
        """)

        self.initiate_not_found_response = loads("""
            {"ok": 2, "operationTime": {"$timestamp": {"t": 1549963040, "i": 1}}, "$clusterTime": {"clusterTime":
            {"$timestamp": {"t": 1549963040, "i": 1}}, "signature": {"hash": {"$binary": "AAAAAAAAAAAAAAAAAAAAAAAAAAA=",
            "$type": "00"}, "keyId": 0}}}
        """)

        self.expected_cluster_config = {
            "_id": "mongo-cluster",
            "version": 1,
            "members": [
                {"_id": 0, "host": "mongo-cluster-0.mongo-cluster.mongo-operator-cluster.svc.cluster.local"},
                {"_id": 1, "host": "mongo-cluster-1.mongo-cluster.mongo-operator-cluster.svc.cluster.local"},
                {"_id": 2, "host": "mongo-cluster-2.mongo-cluster.mongo-operator-cluster.svc.cluster.local"}
            ]
        }

        self.expected_user_create = {
            "pwd": "random-password",
            "roles": [{"role": "root", "db": "admin"}]
        }

    @staticmethod
    def _getFixture(name):
        with open("tests/fixtures/mongo_responses/{}.json".format(name)) as f:
            return loads(f.read())

    def test_mongoAdminCommand(self, mongo_client_mock):
        mongo_client_mock.return_value.admin.command.return_value = self._getFixture("initiate-ok")
        result = self.service._executeAdminCommand(self.cluster_object, "replSetInitiate")
        self.assertEqual(self.initiate_ok_response, result)

    def test__mongoAdminCommand_NodeNotFound(self, mongo_client_mock):
        mongo_client_mock.return_value.admin.command.side_effect = OperationFailure(
            "replSetInitiate quorum check failed because not all proposed set members responded affirmatively:")

        with self.assertRaises(OperationFailure) as ex:
            mongo_command, mongo_args = MongoResources.createReplicaInitiateCommand(self.cluster_object)
            self.service._executeAdminCommand(self.cluster_object, mongo_command, mongo_args)

        self.assertIn("replSetInitiate quorum check failed", str(ex.exception))

    def test__mongoAdminCommand_connect_failed(self, mongo_client_mock):
        mongo_client_mock.return_value.admin.command.side_effect = (
            ConnectionFailure("connection attempt failed"),
            self._getFixture("initiate-ok")
        )
        result = self.service._executeAdminCommand(self.cluster_object, "replSetGetStatus")
        self.assertEqual(self.initiate_ok_response, result)

    def test__mongoAdminCommand_TimeoutError(self, mongo_client_mock):
        mongo_client_mock.return_value.admin.command.side_effect = (
            ConnectionFailure("connection attempt failed"),
            ConnectionFailure("connection attempt failed"),
            ConnectionFailure("connection attempt failed"),
            ConnectionFailure("connection attempt failed"),
            OperationFailure("no replset config has been received")
        )

        with self.assertRaises(TimeoutError) as context:
            self.service._executeAdminCommand(self.cluster_object, "replSetGetStatus")

        self.assertEqual("Could not execute command after 4 retries!", str(context.exception))

    def test__mongoAdminCommand_NoPrimary(self, mongo_client_mock):
        mongo_client_mock.return_value.admin.command.side_effect = (
            ConnectionFailure("No replica set members match selector \"Primary()\""),
            self._getFixture("initiate-ok"),
            self._getFixture("initiate-ok")

        )

        self.service._executeAdminCommand(self.cluster_object, "replSetGetStatus")

    def test_initializeReplicaSet(self, mongo_client_mock):
        mongo_client_mock.return_value.admin.command.return_value = self._getFixture("initiate-ok")
        self.service._initializeReplicaSet(self.cluster_object)

    def test_initializeReplicaSet_ValueError(self, mongo_client_mock):
        command_result = self._getFixture("initiate-ok")
        command_result["ok"] = 2
        mongo_client_mock.return_value.admin.command.return_value = command_result

        with self.assertRaises(ValueError) as context:
            self.service._initializeReplicaSet(self.cluster_object)

        self.assertEqual("Unexpected response initializing replica set mongo-cluster @ ns/"
                         + self.cluster_object.metadata.namespace + ":\n" + str(self.initiate_not_found_response),
                         str(context.exception))

    def test_reconfigureReplicaSet(self, mongo_client_mock):
        mongo_client_mock.return_value.admin.command.return_value = self._getFixture("initiate-ok")
        self.service._reconfigureReplicaSet(self.cluster_object)

    def test_reconfigureReplicaSet_ValueError(self, mongo_client_mock):
        command_result = self._getFixture("initiate-ok")
        command_result["ok"] = 2
        mongo_client_mock.return_value.admin.command.return_value = command_result

        with self.assertRaises(ValueError) as context:
            self.service._reconfigureReplicaSet(self.cluster_object)

        self.assertEqual("Unexpected response reconfiguring replica set mongo-cluster @ ns/mongo-operator-cluster:\n"
                         + str(self.initiate_not_found_response), str(context.exception))

    def test_checkOrCreateReplicaSet_ok(self, mongo_client_mock):
        mongo_client_mock.return_value.admin.command.return_value = self._getFixture("replica-status-ok")
        self.service.checkOrCreateReplicaSet(self.cluster_object)

    def test_checkOrCreateReplicaSet_initialize(self, mongo_client_mock):
        mongo_client_mock.return_value.admin.command.side_effect = (
            OperationFailure("no replset config has been received"),
            self._getFixture("initiate-ok")
        )
        self.service.checkOrCreateReplicaSet(self.cluster_object)

    def test_checkOrCreateReplicaSet_reconfigure(self, mongo_client_mock):
        self.cluster_object.spec.mongodb.replicas = 4
        mongo_client_mock.return_value.admin.command.return_value = self._getFixture("replica-status-ok")
        self.service.checkOrCreateReplicaSet(self.cluster_object)
        self.expected_cluster_config["members"].append({
            "_id": 3,
            "host": "mongo-cluster-3.mongo-cluster.mongo-cluster.svc.cluster.local"
        })

    def test_checkOrCreateReplicaSet_ValueError(self, mongo_client_mock):
        response = self._getFixture("replica-status-ok")
        response["ok"] = 2
        mongo_client_mock.return_value.admin.command.return_value = response

        with self.assertRaises(ValueError) as context:
            self.service.checkOrCreateReplicaSet(self.cluster_object)

        self.assertIn("Unexpected response trying to check replicas: ", str(context.exception))

    def test_checkOrCreateReplicaSet_OperationalFailure(self, mongo_client_mock):
        bad_value = "BadValue: Unexpected field foo in replica set member configuration for member:" \
            "{ _id: 0, foo: \"localhost:27017\" }"
        mongo_client_mock.return_value.admin.command.side_effect = (
            OperationFailure(bad_value),
            OperationFailure(bad_value))

        with self.assertRaises(OperationFailure) as context:
            self.service.checkOrCreateReplicaSet(self.cluster_object)

        self.assertEqual(str(context.exception), bad_value)

    def test_createUsers_ok(self, mongo_client_mock):
        mongo_client_mock.return_value.admin.command.return_value = self._getFixture("createUser-ok")
        self.service.createUsers(self.cluster_object)

    def test_createUsers_ValueError(self, mongo_client_mock):
        mongo_client_mock.return_value.admin.command.side_effect = OperationFailure(
            "\"createUser\" had the wrong type. Expected string, found object"),

        with self.assertRaises(OperationFailure) as context:
            self.service.createUsers(self.cluster_object)

        self.assertEqual("\"createUser\" had the wrong type. Expected string, found object", str(context.exception))

    def test_createUsers_TimeoutError(self, mongo_client_mock):
        mongo_client_mock.return_value.admin.command.side_effect = (
            ConnectionFailure("connection attempt failed"), ConnectionFailure("connection attempt failed"),
            ConnectionFailure("connection attempt failed"), ConnectionFailure("connection attempt failed")
        )

        with self.assertRaises(TimeoutError) as context:
            self.service.createUsers(self.cluster_object)

        self.assertEqual("Could not execute command after 4 retries!", str(context.exception))

    def test_onReplicaSetReady(self, mongo_client_mock):
        self.service._restore_helper.restoreIfNeeded = MagicMock()

        self.service._onReplicaSetReady(self.cluster_object)

        self.service._restore_helper.restoreIfNeeded.assert_called()
        mongo_client_mock.assert_not_called()

    def test_onReplicaSetReady_alreadyRestored(self, mongo_client_mock):
        self.service._restore_helper.restoreIfNeeded = MagicMock()
        self.service._restored_cluster_names.append("mongo-cluster")

        self.service._onReplicaSetReady(self.cluster_object)

        self.service._restore_helper.restoreIfNeeded.assert_not_called()
        mongo_client_mock.assert_not_called()

    def test_onAllHostsReady(self, mongo_client_mock):
        self.service._initializeReplicaSet = MagicMock()

        self.service._onAllHostsReady(self.cluster_object)

        self.service._initializeReplicaSet.assert_called()
        mongo_client_mock.assert_not_called()
