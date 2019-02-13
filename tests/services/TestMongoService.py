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

from mongoOperator.helpers.MongoResources import MongoResources
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService
from mongoOperator.services.MongoService import MongoService
from tests.test_utils import getExampleClusterDefinition
from bson.json_util import loads
from pymongo.errors import OperationFailure, ConnectionFailure

@patch("mongoOperator.services.MongoService.sleep", MagicMock())
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

        self.initiate_ok_response = loads('''
            {"ok": 1.0, "operationTime": {"$timestamp": {"t": 1549963040, "i": 1}}, "$clusterTime": {"clusterTime": 
            {"$timestamp": {"t": 1549963040, "i": 1}}, "signature": {"hash": {"$binary": "AAAAAAAAAAAAAAAAAAAAAAAAAAA=", 
            "$type": "00"}, "keyId": 0}}}
        ''')

        self.initiate_not_found_response = loads('''
            {"ok": 2, "operationTime": {"$timestamp": {"t": 1549963040, "i": 1}}, "$clusterTime": {"clusterTime": 
            {"$timestamp": {"t": 1549963040, "i": 1}}, "signature": {"hash": {"$binary": "AAAAAAAAAAAAAAAAAAAAAAAAAAA=", 
            "$type": "00"}, "keyId": 0}}}
        ''')

        self.expected_cluster_config = {
            "_id": "mongo-cluster",
            "version": 1,
            "members": [
                {"_id": 0, "host": "mongo-cluster-0.mongo-cluster." + self.cluster_object.metadata.namespace +
                                   ".svc.cluster.local"},
                {"_id": 1, "host": "mongo-cluster-1.mongo-cluster." + self.cluster_object.metadata.namespace +
                                   ".svc.cluster.local"},
                {"_id": 2, "host": "mongo-cluster-2.mongo-cluster." + self.cluster_object.metadata.namespace +
                                   ".svc.cluster.local"}
            ]
        }

        self.expected_user_create = {
            "pwd": "random-password",
            "roles": [{"role": "root", "db": "admin"}]
        }

    def _getFixture(self, name):
        with open("tests/fixtures/mongo_responses/{}.json".format(name)) as f:
            return loads(f.read())

    @patch("mongoOperator.services.MongoService.MongoClient")
    def test__mongoAdminCommand(self, mongoclient_mock):
        mongoclient_mock.return_value.admin.command.return_value = self._getFixture("initiate-ok")
        result = self.service._mongoAdminCommand(self.cluster_object, "replSetInitiate")
        self.assertEqual(self.initiate_ok_response, result)
        # expected_calls = [
        #     call(MongoResources.getConnectionSeeds(self.cluster_object), replicaSet=self.cluster_object.metadata.name),
        #     call().admin.command('replSetInitiate')
        # ]
        # self.assertEqual(expected_calls, mongoclient_mock.mock_calls)

    @patch("mongoOperator.services.MongoService.MongoClient")
    def test__mongoAdminCommand_NodeNotFound(self, mongoclient_mock):
        mongoclient_mock.return_value.admin.command.side_effect = OperationFailure("replSetInitiate quorum check failed"
                                                                                   " because not all proposed set "
                                                                                   "members responded affirmatively:")
        with self.assertRaises(OperationFailure) as ex:
            mongo_command, mongo_args = MongoResources.createReplicaInitiateCommand(self.cluster_object)
            self.service._mongoAdminCommand(self.cluster_object, mongo_command, mongo_args)

        # expected = [
        #     call(MongoResources.getConnectionSeeds(self.cluster_object), replicaSet=self.cluster_object.metadata.name),
        #     call().admin.command('replSetInitiate', self.expected_cluster_config)
        # ]
        # self.assertEqual(expected, mongoclient_mock.mock_calls)
        self.assertIn("replSetInitiate quorum check failed", str(ex.exception))

    @patch("mongoOperator.services.MongoService.MongoClient")
    def test__mongoAdminCommand_connect_failed(self, mongoclient_mock):
        mongoclient_mock.return_value.admin.command.side_effect = (
            ConnectionFailure("connection attempt failed"),
            self._getFixture("initiate-ok")
        )
        result = self.service._mongoAdminCommand(self.cluster_object, "replSetGetStatus")
        self.assertEqual(self.initiate_ok_response, result)
        # expected_calls = 2 * [
        #     call(MongoResources.getConnectionSeeds(self.cluster_object), replicaSet=self.cluster_object.metadata.name),
        #     call().admin.command('replSetGetStatus')
        # ]
        # self.assertEqual(expected_calls, mongoclient_mock.mock_calls)

    @patch("mongoOperator.services.MongoService.MongoClient")
    def test__mongoAdminCommand_TimeoutError(self, mongoclient_mock):
        mongoclient_mock.return_value.admin.command.side_effect = (
            ConnectionFailure("connection attempt failed"),
            ConnectionFailure("connection attempt failed"),
            ConnectionFailure("connection attempt failed"),
            ConnectionFailure("connection attempt failed"),
            OperationFailure("no replset config has been received")
        )
        with self.assertRaises(TimeoutError) as context:
            self.service._mongoAdminCommand(self.cluster_object, "replSetGetStatus")

        self.assertEqual("Could not execute command after 4 retries!", str(context.exception))
        # expected_calls = 4 * [
        #     call(MongoResources.getConnectionSeeds(self.cluster_object), replicaSet=self.cluster_object.metadata.name),
        #     call().admin.command('replSetGetStatus')
        # ]
        # self.assertEqual(expected_calls, mongoclient_mock.mock_calls)

    @patch("mongoOperator.services.MongoService.MongoClient")
    def test__mongoAdminCommand_NoPrimary(self, mongoclient_mock):
        mongoclient_mock.return_value.admin.command.side_effect = (
            ConnectionFailure("No replica set members match selector \"Primary()\""),
            self._getFixture("initiate-ok"),
            self._getFixture("initiate-ok")

        )

        self.service._mongoAdminCommand(self.cluster_object, "replSetGetStatus")

        # expected_calls = [
        #     call(MongoResources.getConnectionSeeds(self.cluster_object), replicaSet=self.cluster_object.metadata.name),
        #     call().admin.command('replSetGetStatus'),
        #     call(MongoResources.getMemberHostname(0, self.cluster_object.metadata.name, self.cluster_object.metadata.namespace)),
        #     call().admin.command('replSetInitiate', self.expected_cluster_config),
        #     call(MongoResources.getConnectionSeeds(self.cluster_object), replicaSet=self.cluster_object.metadata.name),
        #     call().admin.command('replSetGetStatus')
        # ]
        # print(repr(mongoclient_mock.mock_calls))
        # print(repr(expected_calls))
        # self.assertEqual(expected_calls, mongoclient_mock.mock_calls)

    @patch("mongoOperator.services.MongoService.MongoClient")
    @patch("mongoOperator.helpers.RestoreHelper.RestoreHelper.restoreIfNeeded")
    def test_initializeReplicaSet(self, restoreifneeded_mock, mongoclient_mock):
        mongoclient_mock.return_value.admin.command.return_value = self._getFixture("initiate-ok")

        self.service.initializeReplicaSet(self.cluster_object)
        # expected_calls = [
        #     call(MongoResources.getMemberHostname(0, self.cluster_object.metadata.name,
        #                                           self.cluster_object.metadata.namespace)),
        #     call().admin.command('replSetInitiate', self.expected_cluster_config)
        # ]
        #
        # self.assertEqual(expected_calls, mongoclient_mock.mock_calls)

    @patch("mongoOperator.services.MongoService.MongoClient")
    def test_initializeReplicaSet_ValueError(self, mongoclient_mock):
        command_result = self._getFixture("initiate-ok")
        command_result["ok"] = 2
        mongoclient_mock.return_value.admin.command.return_value = command_result
        with self.assertRaises(ValueError) as context:
            self.service.initializeReplicaSet(self.cluster_object)

        self.assertEqual("Unexpected response initializing replica set mongo-cluster @ ns/" +
                         self.cluster_object.metadata.namespace + ":\n" +
                         str(self.initiate_not_found_response),
                         str(context.exception))

    @patch("mongoOperator.services.MongoService.MongoClient")
    def test_reconfigureReplicaSet(self, mongoclient_mock):
        mongoclient_mock.return_value.admin.command.return_value = self._getFixture("initiate-ok")

        self.service.reconfigureReplicaSet(self.cluster_object)
        # expected_calls = [
        #     call(MongoResources.getConnectionSeeds(self.cluster_object), replicaSet=self.cluster_object.metadata.name),
        #     call().admin.command('replSetReconfig', self.expected_cluster_config)
        # ]
        # self.assertEqual(expected_calls, mongoclient_mock.mock_calls)

    @patch("mongoOperator.services.MongoService.MongoClient")
    def test_reconfigureReplicaSet_ValueError(self, mongoclient_mock):
        command_result = self._getFixture("initiate-ok")
        command_result["ok"] = 2
        mongoclient_mock.return_value.admin.command.return_value = command_result

        with self.assertRaises(ValueError) as context:
            self.service.reconfigureReplicaSet(self.cluster_object)

        self.assertEqual("Unexpected response reconfiguring replica set mongo-cluster @ ns/" +
                         self.cluster_object.metadata.namespace + ":\n" +
                         str(self.initiate_not_found_response),
                         str(context.exception))

    @patch("mongoOperator.services.MongoService.MongoClient")
    def test_checkReplicaSetOrInitialize_ok(self, mongoclient_mock):
        mongoclient_mock.return_value.admin.command.return_value = self._getFixture("replica-status-ok")
        self.service.checkReplicaSetOrInitialize(self.cluster_object)

        # expected_calls = [
        #     call(MongoResources.getConnectionSeeds(self.cluster_object), replicaSet=self.cluster_object.metadata.name),
        #     call().admin.command('replSetGetStatus')
        # ]
        # self.assertEqual(expected_calls, mongoclient_mock.mock_calls)

    @patch("mongoOperator.services.MongoService.MongoClient")
    @patch("mongoOperator.helpers.RestoreHelper.RestoreHelper.restoreIfNeeded")
    def test_checkReplicaSetOrInitialize_initialize(self, restoreifneeded_mock, mongoclient_mock):
        mongoclient_mock.return_value.admin.command.side_effect = (
            OperationFailure("no replset config has been received"),
            self._getFixture("initiate-ok"))

        self.service.checkReplicaSetOrInitialize(self.cluster_object)

        # expected_calls = [
        #     call(MongoResources.getConnectionSeeds(self.cluster_object), replicaSet=self.cluster_object.metadata.name),
        #     call().admin.command('replSetGetStatus'),
        #     call(MongoResources.getMemberHostname(0, self.cluster_object.metadata.name,
        #                                           self.cluster_object.metadata.namespace)),
        #     call().admin.command('replSetInitiate', self.expected_cluster_config)
        # ]
        #
        # self.assertEqual(expected_calls, mongoclient_mock.mock_calls)

    @patch("mongoOperator.services.MongoService.MongoClient")
    def test_checkReplicaSetOrInitialize_reconfigure(self, mongoclient_mock):
        self.cluster_object.spec.mongodb.replicas = 4
        mongoclient_mock.return_value.admin.command.return_value = self._getFixture("replica-status-ok")
        self.service.checkReplicaSetOrInitialize(self.cluster_object)

        cluster_config = self.expected_cluster_config
        cluster_config["members"].append({"_id": 3, "host": "mongo-cluster-3.mongo-cluster." +
                                                            self.cluster_object.metadata.namespace +
                                                            ".svc.cluster.local"})
        self.expected_cluster_config = cluster_config

        # expected_calls = [
        #     call(MongoResources.getConnectionSeeds(self.cluster_object), replicaSet=self.cluster_object.metadata.name),
        #     call().admin.command('replSetGetStatus'),
        #     call(MongoResources.getConnectionSeeds(self.cluster_object), replicaSet=self.cluster_object.metadata.name),
        #     call().admin.command('replSetReconfig', self.expected_cluster_config)
        # ]
        #
        # self.assertEqual(expected_calls, mongoclient_mock.mock_calls)

    @patch("mongoOperator.services.MongoService.MongoClient")
    def test_checkReplicaSetOrInitialize_ValueError(self, mongoclient_mock):
        response = self._getFixture("replica-status-ok")
        response["ok"] = 2

        mongoclient_mock.return_value.admin.command.return_value = response

        with self.assertRaises(ValueError) as context:
            self.service.checkReplicaSetOrInitialize(self.cluster_object)

        # expected_calls = [
        #     call(MongoResources.getConnectionSeeds(self.cluster_object), replicaSet=self.cluster_object.metadata.name),
        #     call().admin.command('replSetGetStatus')
        # ]
        # self.assertEqual(expected_calls, mongoclient_mock.mock_calls)
        self.assertIn("Unexpected response trying to check replicas: ", str(context.exception))

    @patch("mongoOperator.services.MongoService.MongoClient")
    @patch("mongoOperator.helpers.RestoreHelper.RestoreHelper.restoreIfNeeded")
    def test_checkReplicaSetOrInitialize_OperationalFailure(self, restoreifneeded_mock, mongoclient_mock):
        badvalue = "BadValue: Unexpected field foo in replica set member configuration for member:" \
                   "{ _id: 0, foo: \"localhost:27017\" }"
        mongoclient_mock.return_value.admin.command.side_effect = (
            OperationFailure(badvalue),
            OperationFailure(badvalue))

        with self.assertRaises(OperationFailure) as context:
            self.service.checkReplicaSetOrInitialize(self.cluster_object)
        #
        # expected_calls = [
        #     call(MongoResources.getConnectionSeeds(self.cluster_object), replicaSet=self.cluster_object.metadata.name),
        #     call().admin.command('replSetGetStatus'),
        # ]
        #
        # self.assertEqual(expected_calls, mongoclient_mock.mock_calls)
        self.assertEqual(str(context.exception), badvalue)

    @patch("mongoOperator.services.MongoService.MongoClient")
    def test_createUsers_ok(self, mongoclient_mock):
        mongoclient_mock.return_value.admin.command.return_value = self._getFixture("createUser-ok")

        self.service.createUsers(self.cluster_object)

        # expected_calls = [
        #     call(MongoResources.getConnectionSeeds(self.cluster_object), replicaSet=self.cluster_object.metadata.name),
        #     call().admin.command("createUser", "root", **self.expected_user_create)
        # ]
        # self.assertEqual(expected_calls, mongoclient_mock.mock_calls)

    @patch("mongoOperator.services.MongoService.MongoClient")
    def test_createUsers_ValueError(self, mongoclient_mock):
        mongoclient_mock.return_value.admin.command.side_effect = OperationFailure("\"createUser\" had the wrong type."
                                                                                   " Expected string, found object"),

        with self.assertRaises(OperationFailure) as context:
            self.service.createUsers(self.cluster_object)

        # expected_calls = [
        #     call(MongoResources.getConnectionSeeds(self.cluster_object), replicaSet=self.cluster_object.metadata.name),
        #     call().admin.command("createUser", "root", **self.expected_user_create)
        # ]
        #
        # self.assertEqual(expected_calls, mongoclient_mock.mock_calls)
        self.assertEqual("\"createUser\" had the wrong type. Expected string, found object", str(context.exception))

    @patch("mongoOperator.services.MongoService.MongoClient")
    def test_createUsers_TimeoutError(self, mongoclient_mock):
        mongoclient_mock.return_value.admin.command.side_effect = (ConnectionFailure("connection attempt failed"),
                                                                   ConnectionFailure("connection attempt failed"),
                                                                   ConnectionFailure("connection attempt failed"),
                                                                   ConnectionFailure("connection attempt failed"))

        with self.assertRaises(TimeoutError) as context:
            self.service.createUsers(self.cluster_object)

        # expected_calls = 4 * [
        #     call(MongoResources.getConnectionSeeds(self.cluster_object), replicaSet=self.cluster_object.metadata.name),
        #     call().admin.command("createUser", "root", **self.expected_user_create)
        # ]
        #
        # self.assertEqual(expected_calls, mongoclient_mock.mock_calls)
        self.assertEqual("Could not execute command after 4 retries!", str(context.exception))
