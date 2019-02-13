# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase
from unittest.mock import patch, call
from mongoOperator.helpers.ClusterChecker import ClusterChecker
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from tests.test_utils import getExampleClusterDefinition
from bson.json_util import loads


class TestClusterChecker(TestCase):
    maxDiff = None

    def setUp(self):
        super().setUp()
        with patch("mongoOperator.helpers.ClusterChecker.KubernetesService") as ks:
            self.checker = ClusterChecker()
            self.kubernetes_service = ks.return_value
        self.cluster_dict = getExampleClusterDefinition()
        self.cluster_dict["metadata"]["resourceVersion"] = "100"
        self.cluster_object = V1MongoClusterConfiguration(**self.cluster_dict)

    @staticmethod
    def _getMongoFixture(name):
        with open("tests/fixtures/mongo_responses/{}.json".format(name), "rb") as f:
            return loads(f.read())

    def test___init__(self):
        self.assertEqual(self.kubernetes_service, self.checker._kubernetes_service)
        self.assertEqual(self.kubernetes_service, self.checker._mongo_service._kubernetes_service)
        self.assertEqual(3, len(self.checker._resource_checkers), self.checker._resource_checkers)
        self.assertEqual({}, self.checker._cluster_versions)

    def test__parseConfiguration_ok(self):
        self.assertEqual(self.cluster_object, self.checker._parseConfiguration(self.cluster_dict))

    def test__parseConfiguration_error(self):
        self.assertIsNone(self.checker._parseConfiguration({"invalid": "dict"}))

    def test_checkExistingClusters_empty(self):
        self.kubernetes_service.listMongoObjects.return_value = {"items": []}
        self.checker.checkExistingClusters()
        expected = [call.listMongoObjects()]
        self.assertEqual(expected, self.kubernetes_service.mock_calls)
        self.assertEqual({}, self.checker._cluster_versions)

    def test_checkExistingClusters_bad_format(self):
        self.kubernetes_service.listMongoObjects.return_value = {"items": [{"invalid": "object"}]}
        self.checker.checkExistingClusters()
        expected = [call.listMongoObjects()]
        self.assertEqual(expected, self.kubernetes_service.mock_calls)
        self.assertEqual({}, self.checker._cluster_versions)

    @patch("mongoOperator.services.MongoService.MongoClient")
    @patch("mongoOperator.helpers.BackupChecker.BackupChecker.backupIfNeeded")
    def test_checkExistingClusters(self, backup_mock, mongo_client_mock):
        self.checker._cluster_versions[("mongo-cluster", self.cluster_object.metadata.namespace)] = "100"
        self.kubernetes_service.listMongoObjects.return_value = {"items": [self.cluster_dict]}
        mongo_client_mock.return_value.admin.command.return_value = self._getMongoFixture("replica-status-ok")
        self.checker.checkExistingClusters()
        self.assertEqual({("mongo-cluster", self.cluster_object.metadata.namespace): "100"},
                         self.checker._cluster_versions)
        expected = [call.listMongoObjects()]
        self.assertEqual(expected, self.kubernetes_service.mock_calls)
        backup_mock.assert_called_once_with(self.cluster_object)

    @patch("mongoOperator.helpers.BaseResourceChecker.BaseResourceChecker.cleanResources")
    @patch("mongoOperator.helpers.BaseResourceChecker.BaseResourceChecker.listResources")
    def test_collectGarbage(self, list_mock, clean_mock):
        list_mock.return_value = [self.cluster_object]
        self.checker.collectGarbage()
        self.assertEqual([call()] * 3, clean_mock.mock_calls)
        self.assertEqual([], self.kubernetes_service.mock_calls)  # k8s is not called because we mocked everything

    @patch("mongoOperator.services.MongoService.MongoClient")
    @patch("mongoOperator.helpers.BackupChecker.BackupChecker.backupIfNeeded")
    def test_checkCluster_same_version(self, backup_mock, mongo_client_mock):
        self.checker._cluster_versions[("mongo-cluster", "mongo-operator-cluster")] = "100"
        mongo_client_mock.return_value.admin.command.return_value = self._getMongoFixture("replica-status-ok")
        self.checker._checkCluster(self.cluster_object)
        self.assertEqual({("mongo-cluster", "mongo-operator-cluster"): "100"}, self.checker._cluster_versions)
        backup_mock.assert_called_once_with(self.cluster_object)

    @patch("mongoOperator.services.MongoService.MongoClient")
    @patch("mongoOperator.helpers.BackupChecker.BackupChecker.backupIfNeeded")
    @patch("mongoOperator.helpers.MongoResources.MongoResources.createCreateAdminCommand")
    @patch("mongoOperator.helpers.BaseResourceChecker.BaseResourceChecker.checkResource")
    def test_checkCluster_new_version(self, check_mock, admin_mock, backup_mock, mongo_client_mock):
        admin_mock.return_value = "createUser", "foo", {}
        self.checker._cluster_versions[("mongo-cluster", "mongo-operator-cluster")] = "50"
        mongo_client_mock.return_value.admin.command.side_effect = (self._getMongoFixture("replica-status-ok"),
                                                                    self._getMongoFixture("createUser-ok"))
        self.checker._checkCluster(self.cluster_object)
        self.assertEqual({("mongo-cluster", "mongo-operator-cluster"): "100"}, self.checker._cluster_versions)
        expected = [call.getSecret("mongo-cluster-admin-credentials", "mongo-operator-cluster")]
        self.assertEqual(expected, self.kubernetes_service.mock_calls)
        self.assertEqual([call(self.cluster_object)] * 3, check_mock.mock_calls)
        backup_mock.assert_called_once_with(self.cluster_object)
        self.assertEqual([call(self.kubernetes_service.getSecret())], admin_mock.mock_calls)
