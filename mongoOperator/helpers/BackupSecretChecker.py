# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import List

from kubernetes.client import V1Secret, V1Status

from mongoOperator.helpers.BaseResourceChecker import BaseResourceChecker
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService

from Settings import Settings


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
        return {
            cluster_object.spec.backups.gcs.service_account.secret_key_ref.key: Settings.GOOGLE_SERVICE_CREDENTIALS
        }

    @staticmethod
    def _getSecretName(cluster_object: V1MongoClusterConfiguration) -> str:
        return cluster_object.spec.backups.gcs.service_account.secret_key_ref.name

    def listResources(self) -> List[T]:
        return self.kubernetes_service.listAllSecretsWithLabels().items

    def getResource(self, cluster_object: V1MongoClusterConfiguration) -> T:
        return self.kubernetes_service.getSecret(self._getSecretName(cluster_object), namespace)

    def createResource(self, cluster_object: V1MongoClusterConfiguration) -> T:
        return self.kubernetes_service.createSecret(
            self._getSecretName(cluster_object), cluster_object.metadata.namespace, self._getSecretData(cluster_object)
        )

    def updateResource(self, cluster_object: V1MongoClusterConfiguration) -> T:
        return self.kubernetes_service.updateSecret(
            self._getSecretName(cluster_object), cluster_object.metadata.namespace, self._getSecretData(cluster_object)
        )

    def deleteResource(self, cluster_name: str, namespace: str) -> V1Status:
        return self.kubernetes_service.deleteSecret(self._getSecretName(cluster_object), namespace)
