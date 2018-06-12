# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch, call

from mongoOperator.helpers.ClusterChecker import ClusterChecker
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from tests.test_utils import getExampleClusterDefinition


class TestClusterChecker(TestCase):
    maxDiff = None

    def setUp(self):
        super().setUp()
        with patch("mongoOperator.helpers.ClusterChecker.KubernetesService") as ks:
            self.checker = ClusterChecker()
            self.kubernetes_service = ks.return_value
        self.cluster_dict = getExampleClusterDefinition()
        self.cluster_dict["metadata"]["resource_version"] = "100"
        self.cluster_object = V1MongoClusterConfiguration(**self.cluster_dict)

    def _getMongoFixture(self, name):
        with open("tests/fixtures/mongo_responses/{}.txt".format(name)) as f:
            return f.read()

    def test___init__(self):
        self.assertEqual(self.kubernetes_service, self.checker.kubernetes_service)
        self.assertEqual(self.kubernetes_service, self.checker.mongo_service.kubernetes_service)
        self.assertEqual(4, len(self.checker.resource_checkers), self.checker.resource_checkers)
        self.assertEqual({}, self.checker.cluster_versions)

    def test__parseConfiguration_ok(self):
        self.assertEqual(self.cluster_object, self.checker._parseConfiguration(self.cluster_dict))

    def test__parseConfiguration_error(self):
        self.assertIsNone(self.checker._parseConfiguration({"invalid": "dict"}))

    def test_checkExistingClusters_empty(self):
        self.kubernetes_service.listMongoObjects.return_value = {"items": []}
        self.checker.checkExistingClusters()
        expected = [call.listMongoObjects()]
        self.assertEqual(expected, self.kubernetes_service.mock_calls)
        self.assertEqual({}, self.checker.cluster_versions)

    def test_checkExistingClusters_bad_format(self):
        self.kubernetes_service.listMongoObjects.return_value = {"items": [{"invalid": "object"}]}
        self.checker.checkExistingClusters()
        expected = [call.listMongoObjects()]
        self.assertEqual(expected, self.kubernetes_service.mock_calls)
        self.assertEqual({}, self.checker.cluster_versions)

    @patch("mongoOperator.helpers.BackupChecker.BackupChecker.backupIfNeeded")
    def test_checkExistingClusters(self, backup_mock):
        self.checker.cluster_versions[("mongo-cluster", "default")] = "100"  # checkCluster will assume cached version
        self.kubernetes_service.listMongoObjects.return_value = {"items": [self.cluster_dict]}
        self.kubernetes_service.execInPod.return_value = self._getMongoFixture("replica-status-ok")
        self.checker.checkExistingClusters()
        self.assertEqual({("mongo-cluster", "default"): "100"}, self.checker.cluster_versions)
        expected = [call.listMongoObjects(),
                    call.execInPod('mongodb', 'mongo-cluster-0', 'default',
                                   ['mongo', 'localhost:27017/admin', '--eval', 'rs.status()'])]
        self.assertEqual(expected, self.kubernetes_service.mock_calls)
        backup_mock.assert_called_once_with(self.cluster_object)

    @patch("mongoOperator.helpers.ClusterChecker.ClusterChecker.checkCluster")
    @patch("kubernetes.watch.watch.Watch.stream")
    def test_streamEvents_add_update(self, stream_mock, check_mock):
        updated_cluster = deepcopy(self.cluster_dict)
        updated_cluster["spec"]["mongodb"]["replicas"] = 5
        updated_cluster["metadata"]["resource_version"] = "200"
        stream_mock.return_value = [
            {"type": "ADDED", "object": self.cluster_dict},
            {"type": "MODIFIED", "object": updated_cluster},
        ]

        self.checker.streamEvents()

        self.assertEqual([call(self.cluster_object), call(V1MongoClusterConfiguration(**updated_cluster))],
                         check_mock.mock_calls)
        stream_mock.assert_called_once_with(self.kubernetes_service.listMongoObjects, _request_timeout = 5.0)

    @patch("mongoOperator.helpers.ClusterChecker.Watch")
    def test_streamEvents_bad_event(self, watch_mock):
        stream_mock = watch_mock.return_value.stream
        stream_mock.return_value = [{"type": "UNKNOWN", "object": self.cluster_dict}]
        self.checker.streamEvents()
        stream_mock.assert_called_once_with(self.kubernetes_service.listMongoObjects, _request_timeout = 5.0)
        self.assertTrue(watch_mock.return_value.stop)

    @patch("mongoOperator.helpers.ClusterChecker.ClusterChecker.collectGarbage")
    @patch("kubernetes.watch.watch.Watch.stream")
    def test_streamEvents_delete(self, stream_mock, garbage_mock):
        stream_mock.return_value = [{"type": "DELETED"}]
        self.checker.streamEvents()
        garbage_mock.assert_called_once_with()
        stream_mock.assert_called_once_with(self.kubernetes_service.listMongoObjects, _request_timeout = 5.0)

    @patch("mongoOperator.helpers.ClusterChecker.Watch")
    def test_streamEvents_bad_cluster(self, watch_mock):
        self.checker.cluster_versions[("mongo-cluster", "default")] = "100"
        stream_mock = watch_mock.return_value.stream
        stream_mock.return_value = [{"type": "ADDED", "object": {"thisIsNot": "a_cluster"}}]
        self.checker.streamEvents()
        stream_mock.assert_called_once_with(self.kubernetes_service.listMongoObjects, _request_timeout = 5.0)
        self.assertTrue(watch_mock.return_value.stop)
        self.assertEquals("100", watch_mock.return_value.resource_version)

    @patch("mongoOperator.helpers.BaseResourceChecker.BaseResourceChecker.cleanResources")
    @patch("mongoOperator.helpers.BaseResourceChecker.BaseResourceChecker.listResources")
    def test_collectGarbage(self, list_mock, clean_mock):
        list_mock.return_value = [self.cluster_object]
        self.checker.collectGarbage()
        self.assertEqual([call()] * 3, clean_mock.mock_calls)
        self.assertEqual([], self.kubernetes_service.mock_calls)  # k8s is not called because we mocked everything

    @patch("mongoOperator.helpers.BackupChecker.BackupChecker.backupIfNeeded")
    def test_checkCluster_same_version(self, backup_mock):
        self.checker.cluster_versions[("mongo-cluster", "default")] = "100"  # checkCluster will assume cached version
        self.kubernetes_service.execInPod.return_value = self._getMongoFixture("replica-status-ok")
        self.checker.checkCluster(self.cluster_object)
        self.assertEqual({("mongo-cluster", "default"): "100"}, self.checker.cluster_versions)
        expected = [call.execInPod('mongodb', 'mongo-cluster-0', 'default',
                                   ['mongo', 'localhost:27017/admin', '--eval', 'rs.status()'])]
        self.assertEqual(expected, self.kubernetes_service.mock_calls)
        backup_mock.assert_called_once_with(self.cluster_object)

    @patch("mongoOperator.helpers.BackupChecker.BackupChecker.backupIfNeeded")
    @patch("mongoOperator.helpers.MongoResources.MongoResources.createCreateAdminCommand")
    @patch("mongoOperator.helpers.BaseResourceChecker.BaseResourceChecker.checkResource")
    def test_checkCluster_new_version(self, check_mock, admin_mock, backup_mock):
        self.checker.cluster_versions[("mongo-cluster", "default")] = "50"
        self.kubernetes_service.execInPod.side_effect = (self._getMongoFixture("replica-status-ok"),
                                                         self._getMongoFixture("createUser-exists"))
        self.checker.checkCluster(self.cluster_object)
        self.assertEqual({("mongo-cluster", "default"): "100"}, self.checker.cluster_versions)
        expected = [call.execInPod('mongodb', 'mongo-cluster-0', 'default',
                                   ['mongo', 'localhost:27017/admin', '--eval', 'rs.status()']),
                    call.getOperatorAdminSecret('mongo-cluster', 'default'),
                    call.execInPod('mongodb', 'mongo-cluster-0', 'default', [
                        'mongo', 'localhost:27017/admin', '--eval', admin_mock.return_value
                    ])]
        self.assertEqual(expected, self.kubernetes_service.mock_calls)
        self.assertEqual([call(self.cluster_object)] * 4, check_mock.mock_calls)
        backup_mock.assert_called_once_with(self.cluster_object)
