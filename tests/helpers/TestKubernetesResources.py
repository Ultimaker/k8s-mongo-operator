# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase

import kubernetes

from mongoOperator.helpers.KubernetesResources import KubernetesResources


class TestEventManager(TestCase):

    def test_createRandomPassword(self):
        random_password = KubernetesResources.createRandomPassword()
        self.assertEqual(len(random_password), 32)

    def test_createDefaultLabels(self):
        default_labels = KubernetesResources.createDefaultLabels("my-name")
        self.assertEqual(default_labels["name"], "my-name")
        self.assertEqual(default_labels["heritage"], "mongo")
        self.assertEqual(default_labels["operated-by"], "operators.ultimaker.com")

    def test_createSecret(self):
        secret = KubernetesResources.createSecret("my-secret", "my_namespace", {"password": "secret"})
        self.assertEqual(secret.metadata.name, "my-secret")
        self.assertEqual(secret.metadata.namespace, "my_namespace")
        self.assertEqual(secret.metadata.labels, KubernetesResources.createDefaultLabels("my-secret"))
        self.assertEqual(secret.string_data, {"password": "secret"})

    def test_createService(self):
        fake_cluster_object = kubernetes.client.V1beta1CustomResourceDefinition(kind="Mongo", metadata={
            "name": "my-mongo-set",
            "namespace": "my_namespace"
        })
        service = KubernetesResources.createService(fake_cluster_object)
        self.assertEqual(service.metadata.name, "my-mongo-set")
        self.assertEqual(service.metadata.namespace, "my_namespace")
        self.assertEqual(service.metadata.labels, KubernetesResources.createDefaultLabels("my-mongo-set"))
        self.assertEqual(service.spec.ports[0].name, "mongod")
        self.assertEqual(service.spec.ports[0].port, 27017)

    def test_createStatefulSet(self):
        fake_cluster_object = kubernetes.client.V1beta1CustomResourceDefinition(kind="Mongo", metadata={
            "name": "my-mongo-set",
            "namespace": "my_namespace"
        }, spec={
            "mongodb": {
                "replicas": 3,
                "cpu_limit": "100m",
                "memory_limit": "64Mi"
            }
        })
        stateful_set = KubernetesResources.createStatefulSet(fake_cluster_object)
        print("stateful_set", stateful_set)
