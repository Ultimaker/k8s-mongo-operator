# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase
from unittest.mock import MagicMock, call

from kubernetes.client.rest import ApiException

from mongoOperator.helpers.BaseResourceChecker import BaseResourceChecker
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from tests.test_utils import getExampleClusterDefinition


class TestBaseResourceChecker(TestCase):
    maxDiff = None

    def setUp(self):
        self.kubernetes_service = MagicMock()
        self.checker = BaseResourceChecker(self.kubernetes_service)
        self.cluster_object = V1MongoClusterConfiguration(**getExampleClusterDefinition())

    def test_getClusterName(self):
        self.assertEqual("", self.checker.getClusterName(""))
        self.assertEqual("cluster", self.checker.getClusterName("cluster"))
        self.assertEqual("mongodb cluster", self.checker.getClusterName("mongodb cluster"))
        self.assertEqual([], self.kubernetes_service.mock_calls)

    def test_checkResource_create(self):
        self.checker.getResource = MagicMock(side_effect=ApiException(404))
        self.checker.createResource = MagicMock()
        result = self.checker.checkResource(self.cluster_object)
        self.assertEqual(self.checker.createResource.return_value, result)
        self.checker.createResource.assert_called_once_with(self.cluster_object)
        self.assertEqual([], self.kubernetes_service.mock_calls)

    def test_checkResource_update(self):
        self.checker.getResource = MagicMock()
        self.checker.updateResource = MagicMock()
        result = self.checker.checkResource(self.cluster_object)
        self.assertEqual(self.checker.updateResource.return_value, result)
        self.checker.updateResource.assert_called_once_with(self.cluster_object)
        self.assertEqual([], self.kubernetes_service.mock_calls)

    def test_checkResource_error(self):
        self.checker.getResource = MagicMock(side_effect=ApiException(400))
        with self.assertRaises(ApiException):
            self.checker.checkResource(self.cluster_object)
        self.assertEqual([], self.kubernetes_service.mock_calls)

    def test_cleanResources_empty(self):
        self.checker.listResources = MagicMock(return_value=[])
        self.checker.cleanResources()
        self.assertEqual([], self.kubernetes_service.mock_calls)

    def test_cleanResources_found(self):
        self.kubernetes_service.getMongoObject.return_value = self.cluster_object
        self.checker.listResources = MagicMock(return_value=[self.cluster_object])
        self.checker.cleanResources()
        self.assertEqual([call.getMongoObject('mongo-cluster', 'default')], self.kubernetes_service.mock_calls)

    def test_cleanResources_not_found(self):
        self.kubernetes_service.getMongoObject.side_effect = ApiException(404)
        self.checker.listResources = MagicMock(return_value=[self.cluster_object])
        self.checker.deleteResource = MagicMock()
        self.checker.cleanResources()
        self.assertEqual([call.getMongoObject('mongo-cluster', 'default')], self.kubernetes_service.mock_calls)
        self.checker.deleteResource.assert_called_once_with('mongo-cluster', 'default')

    def test_cleanResources_error(self):
        self.kubernetes_service.getMongoObject.side_effect = ApiException(400)
        self.checker.listResources = MagicMock(return_value=[self.cluster_object])
        with self.assertRaises(ApiException):
            self.checker.cleanResources()
        self.assertEqual([call.getMongoObject('mongo-cluster', 'default')], self.kubernetes_service.mock_calls)

    def test_listResources(self):
        with self.assertRaises(NotImplementedError):
            self.checker.listResources()

    def test_getResource(self):
        with self.assertRaises(NotImplementedError):
            self.checker.getResource(self.cluster_object)

    def test_createResource(self):
        with self.assertRaises(NotImplementedError):
            self.checker.createResource(self.cluster_object)

    def test_updateResource(self):
        with self.assertRaises(NotImplementedError):
            self.checker.updateResource(self.cluster_object)

    def test_deleteResource(self):
        with self.assertRaises(NotImplementedError):
            self.checker.deleteResource("name", "namespace")
