# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase

import kubernetes

from mongoOperator.helpers.KubernetesResources import KubernetesResources


class TestEventManager(TestCase):

    def test_createRandomPassword(self):
        random_password = KubernetesResources.createRandomPassword()
        assert len(random_password) == 32

    def test_createDefaultLabels(self):
        default_labels = KubernetesResources.createDefaultLabels("my-name")
        assert default_labels["name"] == "my-name"
        assert default_labels["heritage"] == "mongo"
        assert default_labels["operated-by"] == "operators.ultimaker.com"

    def test_createSecret(self):
        secret = KubernetesResources.createSecret("my-secret", "my_namespace", {"password": "secret"})
        assert secret.metadata.name == "my-secret"
        assert secret.metadata.namespace == "my_namespace"
        assert secret.metadata.labels == KubernetesResources.createDefaultLabels("my-secret")
        assert secret.string_data == {"password": "secret"}

    def test_createService(self):
        fake_cluster_object = kubernetes.client.V1beta1CustomResourceDefinition(kind="Mongo", metadata={
            "name": "my-mongo-set",
            "namespace": "my_namespace"
        })
        service = KubernetesResources.createService(fake_cluster_object)
        assert service.metadata.name == "my-mongo-set"
        assert service.metadata.namespace == "my_namespace"
        assert service.metadata.labels == KubernetesResources.createDefaultLabels("my-mongo-set")
        assert service.spec.ports[0].name == "mongod"
        assert service.spec.ports[0].port == 27017
