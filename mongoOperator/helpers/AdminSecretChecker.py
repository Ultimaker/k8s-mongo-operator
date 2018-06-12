# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from base64 import b64encode

from kubernetes.client import V1Secret, V1Status
from typing import List, Dict

from mongoOperator.helpers.BaseResourceChecker import BaseResourceChecker
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration


class AdminSecretChecker(BaseResourceChecker):
    """
    Class responsible for handling the operator admin secrets for the Mongo cluster.
    The inherited methods do not have documentation, see the parent class for more details.
    """

    T = V1Secret

    # Name of the secret for each cluster.
    NAME_FORMAT = "{}-admin-credentials"

    @classmethod
    def getClusterName(cls, resource_name: str) -> str:
        return resource_name.replace(cls.NAME_FORMAT.format(""), "")

    @staticmethod
    def _generateSecretData() -> Dict[str, str]:
        """Generates a root user with a random secure password to use in secrets."""
        return {"username": "root", "password": b64encode(os.urandom(33)).decode()}

    def listResources(self) -> List[T]:
        return self.kubernetes_service.listAllSecretsWithLabels().items

    def getResource(self, cluster_object: V1MongoClusterConfiguration) -> T:
        name = self.NAME_FORMAT.format(cluster_object.metadata.name)
        return self.kubernetes_service.getSecret(name, cluster_object.metadata.namespace)

    def createResource(self, cluster_object: V1MongoClusterConfiguration) -> T:
        name = self.NAME_FORMAT.format(cluster_object.metadata.name)
        return self.kubernetes_service.createSecret(name, cluster_object.metadata.namespace, self._generateSecretData())

    def updateResource(self, cluster_object: V1MongoClusterConfiguration) -> T:
        name = self.NAME_FORMAT.format(cluster_object.metadata.name)
        return self.kubernetes_service.updateSecret(name, cluster_object.metadata.namespace, self._generateSecretData())

    def deleteResource(self, cluster_name: str, namespace: str) -> V1Status:
        secret_name = self.NAME_FORMAT.format(cluster_name)
        return self.kubernetes_service.deleteSecret(secret_name, namespace)
