# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase

from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from tests.test_utils import getExampleClusterDefinition


class TestV1MongoClusterConfiguration(TestCase):
    maxDiff = None

    def setUp(self):
        self.cluster_dict = getExampleClusterDefinition()
        self.cluster_object = V1MongoClusterConfiguration(**self.cluster_dict)

    def test_example(self):
        self.cluster_dict["api_version"] = self.cluster_dict.pop("apiVersion")
        self.cluster_dict["spec"]["backups"]["gcs"]["service_account"]["value_from"] = \
            self.cluster_dict["spec"]["backups"]["gcs"]["service_account"].pop("valueFrom")
        self.assertEquals(self.cluster_dict, self.cluster_object.to_dict())

    def test_equals(self):
        self.assertEquals(self.cluster_object, V1MongoClusterConfiguration(**self.cluster_dict))

    def test_example_repr(self):
        expected = \
            "V1MongoClusterConfiguration(api_version=operators.ultimaker.com/v1, kind=Mongo, " \
            "metadata={'name': 'mongo-cluster', 'namespace': 'default'}, " \
            "spec={'backups': {'cron': '0 * * * *', 'gcs': {'bucket': 'mongo-backups', 'service_account': " \
            "{'name': 'MONGO_SERVICE_ACCOUNT', 'value_from': {'secretKeyRef': " \
            "{'key': 'json', 'name': 'storage-serviceaccount'}}}}}, 'mongodb': " \
            "{'cpu_limit': '100m', 'memory_limit': '64Mi', 'replicas': 3}})"
        self.assertEquals(expected, repr(self.cluster_object))

    def test_wrong_replicas(self):
        self.cluster_dict["spec"]["mongodb"]["replicas"] = 2
        with self.assertRaises(ValueError) as context:
            V1MongoClusterConfiguration(**self.cluster_dict)
        self.assertEqual("The amount of replica sets must be between 3 and 50 (got 2).", str(context.exception))
