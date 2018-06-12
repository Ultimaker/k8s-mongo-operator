# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import List

from kubernetes.client import V1Service, V1Status

from mongoOperator.helpers.BaseResourceChecker import BaseResourceChecker
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration


class ServiceChecker(BaseResourceChecker):
    """
    Class responsible for handling the services for the Mongo cluster.
    The inherited methods do not have documentation, see the parent class for more details.
    """

    T = V1Service

    def listResources(self) -> List[T]:
        return self.kubernetes_service.listAllServicesWithLabels().items

    def getResource(self, cluster_object: V1MongoClusterConfiguration) -> T:
        return self.kubernetes_service.getService(cluster_object.metadata.name, cluster_object.metadata.namespace)

    def createResource(self, cluster_object: V1MongoClusterConfiguration) -> T:
        return self.kubernetes_service.createService(cluster_object)

    def updateResource(self, cluster_object: V1MongoClusterConfiguration) -> T:
        return self.kubernetes_service.updateService(cluster_object)

    def deleteResource(self, cluster_name: str, namespace: str) -> V1Status:
        return self.kubernetes_service.deleteService(cluster_name, namespace)
