# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from typing import List, Dict

from kubernetes.client import V1Secret, V1Status

from mongoOperator.helpers.BaseResourceChecker import BaseResourceChecker
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration


class BackupSecretChecker(BaseResourceChecker):
    """
    Class responsible for handling the backup secrets for the Mongo cluster.
    The inherited methods do not have documentation, see the parent class for more details.
    """

    T = V1Secret

    def cleanResources(self):
        logging.warning("TODO: Cleanup backup secrets")  # TODO

    @staticmethod
    def getClusterName(resource_name: str) -> str:
        raise NotImplementedError

    @staticmethod
    def _getSecretData(cluster_object: V1MongoClusterConfiguration) -> Dict[str, str]:
        key = cluster_object.spec.backups.gcs.service_account.secret_key_ref["key"]
        with open("google_credentials.json") as f:
            secret_data = f.read()
        return {key: secret_data}

    @staticmethod
    def _getSecretName(cluster_object: V1MongoClusterConfiguration) -> str:
        return cluster_object.spec.backups.gcs.service_account.secret_key_ref["name"]

    def listResources(self) -> List[T]:
        return self.kubernetes_service.listAllSecretsWithLabels().items

    def getResource(self, cluster_object: V1MongoClusterConfiguration) -> T:
        return self.kubernetes_service.getSecret(self._getSecretName(cluster_object), cluster_object.metadata.namespace)

    def createResource(self, cluster_object: V1MongoClusterConfiguration) -> T:
        return self.kubernetes_service.createSecret(
            self._getSecretName(cluster_object), cluster_object.metadata.namespace, self._getSecretData(cluster_object)
        )

    def updateResource(self, cluster_object: V1MongoClusterConfiguration) -> T:
        return self.kubernetes_service.updateSecret(
            self._getSecretName(cluster_object), cluster_object.metadata.namespace, self._getSecretData(cluster_object)
        )

    def deleteResource(self, cluster_name: str, namespace: str) -> V1Status:
        raise NotImplementedError
