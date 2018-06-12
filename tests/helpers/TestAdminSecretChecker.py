# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase
from unittest.mock import MagicMock

from mongoOperator.helpers.AdminSecretChecker import AdminSecretChecker
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from tests.test_utils import getExampleClusterDefinition


class TestAdminSecretChecker(TestCase):

    def setUp(self):
        super().setUp()
        self.kubernetes_service = MagicMock()
        self.checker = AdminSecretChecker(self.kubernetes_service)
        self.cluster_object = V1MongoClusterConfiguration(**getExampleClusterDefinition())

    def test_getClusterName(self):
        self.assertEqual("mongo_cluster", self.checker.getClusterName("mongo_cluster-admin-credentials"))

    def test_listResources(self):
        result = self.checker.listResources()
        self.assertEqual(self.kubernetes_service.listAllSecretsWithLabels.return_value.items, result)
        self.kubernetes_service.listAllSecretsWithLabels.assert_called_once_with()

    def test_getResource(self):
        result = self.checker.getResource(self.cluster_object)
        self.kubernetes_service.getOperatorAdminSecret.assert_called_once_with(self.cluster_object.metadata.name,
                                                                               self.cluster_object.metadata.namespace)
        self.assertEqual(self.kubernetes_service.getOperatorAdminSecret.return_value, result)

    def test_createResource(self):
        result = self.checker.createResource(self.cluster_object)
        self.assertEqual(self.kubernetes_service.createOperatorAdminSecret.return_value, result)
        self.kubernetes_service.createOperatorAdminSecret.assert_called_once_with(self.cluster_object)

    def test_updateResource(self):
        result = self.checker.updateResource(self.cluster_object)
        self.assertEqual(self.kubernetes_service.updateOperatorAdminSecret.return_value, result)
        self.kubernetes_service.updateOperatorAdminSecret.assert_called_once_with(self.cluster_object)

    def test_deleteResource(self):
        result = self.checker.deleteResource(self.cluster_object.metadata.name,
                                             self.cluster_object.metadata.namespace)
        self.assertEqual(self.kubernetes_service.deleteOperatorAdminSecret.return_value, result)
        self.kubernetes_service.deleteOperatorAdminSecret.assert_called_once_with(
            self.cluster_object.metadata.name, self.cluster_object.metadata.namespace
        )
