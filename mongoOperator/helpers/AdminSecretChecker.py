# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import List

from kubernetes.client import V1Secret, V1Status

from mongoOperator.helpers.BaseResourceChecker import BaseResourceChecker
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService


class AdminSecretChecker(BaseResourceChecker):
    """
    Class responsible for handling the operator admin secrets for the Mongo cluster.
    The inherited methods do not have documentation, see the parent class for more details.
    """

    T = V1Secret

    @staticmethod
    def getClusterName(resource_name: str) -> str:
        return resource_name.replace(KubernetesService.OPERATOR_ADMIN_SECRET_FORMAT.format(""), "")

    def listResources(self) -> List[T]:
        return self.kubernetes_service.listAllSecretsWithLabels().items

    def getResource(self, cluster_name: str, namespace: str) -> T:
        return self.kubernetes_service.getOperatorAdminSecret(cluster_name, namespace)

    def createResource(self, cluster_object: V1MongoClusterConfiguration) -> T:
        return self.kubernetes_service.createOperatorAdminSecret(cluster_object)

    def updateResource(self, cluster_object: V1MongoClusterConfiguration) -> T:
        return self.kubernetes_service.updateOperatorAdminSecret(cluster_object)

    def deleteResource(self, cluster_name: str, namespace: str) -> V1Status:
        return self.kubernetes_service.deleteOperatorAdminSecret(cluster_name, namespace)
