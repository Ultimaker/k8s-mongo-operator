# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import cast
from unittest import TestCase
from unittest.mock import MagicMock, patch

from mongoOperator.helpers.AdminSecretChecker import AdminSecretChecker
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService
from tests.test_utils import getExampleClusterDefinition


class TestAdminSecretChecker(TestCase):

    def setUp(self):
        super().setUp()
        self.kubernetes_service = MagicMock()
        self.checker = AdminSecretChecker(cast(KubernetesService, self.kubernetes_service))
        self.cluster_object = V1MongoClusterConfiguration(**getExampleClusterDefinition())
        self.secret_name = self.cluster_object.metadata.name + "-admin-credentials"

    def test_getClusterName(self):
        self.assertEqual("mongo_cluster", self.checker.getClusterName("mongo_cluster-admin-credentials"))

    def test_listResources(self):
        result = self.checker.listResources()
        self.assertEqual(self.kubernetes_service.listAllSecretsWithLabels.return_value.items, result)
        self.kubernetes_service.listAllSecretsWithLabels.assert_called_once_with()

    def test_getResource(self):
        result = self.checker.getResource(self.cluster_object)
        self.kubernetes_service.getSecret.assert_called_once_with(self.secret_name,
                                                                  self.cluster_object.metadata.namespace)
        self.assertEqual(self.kubernetes_service.getSecret.return_value, result)

    @patch("mongoOperator.helpers.AdminSecretChecker.b64encode")
    def test_createResource(self, b64encode_mock):
        b64encode_mock.return_value = b"random-password"
        result = self.checker.createResource(self.cluster_object)
        self.assertEqual(self.kubernetes_service.createSecret.return_value, result)
        self.kubernetes_service.createSecret.assert_called_once_with(
            self.secret_name, self.cluster_object.metadata.namespace, {"username": "root",
                                                                       "password": "random-password"}
        )

    @patch("mongoOperator.helpers.AdminSecretChecker.b64encode")
    def test_updateResource(self, b64encode_mock):
        b64encode_mock.return_value = b"random-password"
        result = self.checker.updateResource(self.cluster_object)
        self.assertEqual(self.kubernetes_service.updateSecret.return_value, result)
        self.kubernetes_service.updateSecret.assert_called_once_with(
            self.secret_name, self.cluster_object.metadata.namespace, {"username": "root",
                                                                       "password": "random-password"}
        )

    def test_deleteResource(self):
        result = self.checker.deleteResource(self.cluster_object.metadata.name,
                                             self.cluster_object.metadata.namespace)
        self.assertEqual(self.kubernetes_service.deleteSecret.return_value, result)
        self.kubernetes_service.deleteSecret.assert_called_once_with(
            self.secret_name, self.cluster_object.metadata.namespace
        )

    def test__generateSecretData(self):
        result = self.checker._generateSecretData()
        self.assertEqual({"username", "password"}, set(result.keys()))
        self.assertEqual("root", result["username"])
        self.assertEqual(44, len(result["password"]))
        self.assertIsInstance(result["password"], str)
