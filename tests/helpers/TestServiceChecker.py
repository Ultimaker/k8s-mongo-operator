# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import cast
from unittest import TestCase
from unittest.mock import MagicMock

from mongoOperator.helpers.resourceCheckers.ServiceChecker import ServiceChecker
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService
from tests.test_utils import getExampleClusterDefinition


class TestServiceChecker(TestCase):

    def setUp(self):
        super().setUp()
        self.kubernetes_service = MagicMock()
        self.checker = ServiceChecker(cast(KubernetesService, self.kubernetes_service))
        self.cluster_object = V1MongoClusterConfiguration(**getExampleClusterDefinition())

    def test_listResources(self):
        result = self.checker.listResources()
        self.assertEqual(self.kubernetes_service.listAllServicesWithLabels.return_value.items, result)
        self.kubernetes_service.listAllServicesWithLabels.assert_called_once_with()

    def test_getResource(self):
        result = self.checker.getResource(self.cluster_object)
        self.kubernetes_service.getService.assert_called_once_with(self.cluster_object.metadata.name,
                                                                   self.cluster_object.metadata.namespace)
        self.assertEqual(self.kubernetes_service.getService.return_value, result)

    def test_createResource(self):
        result = self.checker.createResource(self.cluster_object)
        self.assertEqual(self.kubernetes_service.createService.return_value, result)
        self.kubernetes_service.createService.assert_called_once_with(self.cluster_object)

    def test_updateResource(self):
        result = self.checker.updateResource(self.cluster_object)
        self.assertEqual(self.kubernetes_service.updateService.return_value, result)
        self.kubernetes_service.updateService.assert_called_once_with(self.cluster_object)

    def test_deleteResource(self):
        result = self.checker.deleteResource(self.cluster_object.metadata.name,
                                             self.cluster_object.metadata.namespace)
        self.assertEqual(self.kubernetes_service.deleteService.return_value, result)
        self.kubernetes_service.deleteService.assert_called_once_with(
            self.cluster_object.metadata.name, self.cluster_object.metadata.namespace
        )
