# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import List

from kubernetes.client import V1StatefulSet, V1Status

from mongoOperator.helpers.BaseResourceChecker import BaseResourceChecker
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration


class StatefulSetChecker(BaseResourceChecker):
    """
    Class responsible for handling the stateful sets for the Mongo cluster.
    The inherited methods do not have documentation, see the parent class for more details.
    """

    T = V1StatefulSet

    def listResources(self) -> List[T]:
        return self.kubernetes_service.listAllStatefulSetsWithLabels().items

    def getResource(self, cluster_name: str, namespace: str) -> T:
        return self.kubernetes_service.getStatefulSet(cluster_name, namespace)

    def createResource(self, cluster_object: V1MongoClusterConfiguration) -> T:
        return self.kubernetes_service.createStatefulSet(cluster_object)

    def updateResource(self, cluster_object: V1MongoClusterConfiguration) -> T:
        return self.kubernetes_service.updateStatefulSet(cluster_object)

    def deleteResource(self, cluster_name: str, namespace: str) -> V1Status:
        return self.kubernetes_service.deleteStatefulSet(cluster_name, namespace)
