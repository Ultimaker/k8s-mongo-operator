# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase

from kubernetes.client import V1SecretKeySelector

from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.models.V1MongoClusterConfigurationSpecMongoDB import V1MongoClusterConfigurationSpecMongoDB
from mongoOperator.models.V1ServiceAccountRef import V1ServiceAccountRef
from tests.test_utils import getExampleClusterDefinition


class TestV1MongoClusterConfiguration(TestCase):
    maxDiff = None

    def setUp(self):
        self.cluster_dict = getExampleClusterDefinition()
        self.cluster_object = V1MongoClusterConfiguration(**self.cluster_dict)

    def test_example(self):
        self.cluster_dict["api_version"] = self.cluster_dict.pop("apiVersion")
        self.cluster_dict["spec"]["backups"]["gcs"]["service_account"] = \
            self.cluster_dict["spec"]["backups"]["gcs"].pop("serviceAccount")
        self.cluster_dict["spec"]["backups"]["gcs"]["service_account"]["secret_key_ref"] = \
            self.cluster_dict["spec"]["backups"]["gcs"]["service_account"].pop("secretKeyRef")
        self.assertEqual(self.cluster_dict, self.cluster_object.to_dict())

    def test_wrong_values_kubernetes_field(self):
        self.cluster_dict["metadata"] = {"invalid": "value"}
        with self.assertRaises(ValueError) as context:
            V1MongoClusterConfiguration(**self.cluster_dict)
        self.assertEqual("Invalid values passed to V1ObjectMeta field: __init__() got an unexpected keyword argument "
                         "'invalid'. Received {'invalid': 'value'}.", str(context.exception))

    def test_embedded_field_none(self):
        del self.cluster_dict["metadata"]
        self.cluster_object.metadata = None
        self.assertEqual(self.cluster_object.to_dict(skip_validation=True),
                         V1MongoClusterConfiguration(**self.cluster_dict).to_dict(skip_validation=True))

    def test_non_required_fields(self):
        cluster_dict = getExampleClusterDefinition(replicas=5)
        cluster_object = V1MongoClusterConfiguration(**cluster_dict)
        self.assertEqual(dict(replicas=5), cluster_dict["spec"]["mongodb"])
        self.assertEqual(V1MongoClusterConfigurationSpecMongoDB(replicas=5), cluster_object.spec.mongodb)

    def test_storage_class_name(self):
        self.cluster_dict["spec"]["mongodb"]["storage_class_name"] = "fast"
        self.cluster_object.spec.mongodb.storage_class_name = "fast"
        self.assertEqual(self.cluster_object.to_dict(skip_validation = True),
                         V1MongoClusterConfiguration(**self.cluster_dict).to_dict(skip_validation = True))

    def test_secret_key_ref(self):
        service_account = self.cluster_object.spec.backups.gcs.service_account
        expected = V1ServiceAccountRef(secret_key_ref=V1SecretKeySelector(name="storage-serviceaccount", key="json"))
        self.assertEqual(expected, service_account)

    def test_equals(self):
        self.assertEqual(self.cluster_object, V1MongoClusterConfiguration(**self.cluster_dict))

    def test_example_repr(self):
        expected = \
            "V1MongoClusterConfiguration(api_version=operators.ultimaker.com/v1, kind=Mongo, " \
            "metadata={'labels': {'app': 'mongo-cluster'}, 'name': 'mongo-cluster', 'namespace': '" \
            + self.cluster_object.metadata.namespace + "'}, " \
            "spec={'backups': {'cron': '0 * * * *', 'gcs': {'bucket': 'ultimaker-mongo-backups', " \
            "'prefix': 'test-backups', 'service_account': {'secret_key_ref': {'key': 'json', " \
            "'name': 'storage-serviceaccount'}}}}, 'mongodb': {'cpu_limit': '100m', 'memory_limit': '64Mi', " \
            "'replicas': 3}})"
        self.assertEqual(expected, repr(self.cluster_object))

    def test_wrong_replicas(self):
        self.cluster_dict["spec"]["mongodb"]["replicas"] = 2
        with self.assertRaises(ValueError) as context:
            V1MongoClusterConfiguration(**self.cluster_dict)
        self.assertEqual("The amount of replica sets must be between 3 and 50 (got 2).", str(context.exception))
