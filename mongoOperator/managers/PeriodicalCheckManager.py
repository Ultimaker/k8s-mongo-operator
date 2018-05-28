# Copyright (c) 2018 Chris ter Beke
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from mongoOperator.managers.Manager import Manager
from mongoOperator.services.KubernetesService import KubernetesService


class PeriodicalCheckManager(Manager):
    """
    Manager that periodically checks the status of the MongoDB objects in the cluster.
    """
    
    CACHED_RESOURCES = {}

    kubernetes_service = KubernetesService()
    
    def _execute(self):
        """Execute the manager logic."""
        self._checkExisting()
        self._collectGarbage()
        
    def _beforeShuttingDown(self):
        """Abstract method we don't need in this manager."""
        pass

    def _checkExisting(self):
        pass
    
    def _collectGarbage(self):
        pass
    
    def _isCachedResource(self, resource) -> bool:
        """
        Check if we have cached the specific version of the given resource locally.
        This limits the amount of calls to the Kubernetes API.
        :param resource: Kubernetes cluster resource to check cache for.
        :return: True if cached, False otherwise.
        """
        uid = resource.metadata.uid
        version = resource.metadata.resource_version
        return uid in self.CACHED_RESOURCES and self.CACHED_RESOURCES[uid] == version

    def _cacheResource(self, resource) -> None:
        """
        Cache a resource by version locally.
        This limits the amount of calls to the Kubernetes API.
        :param resource: Kubernetes cluster resource to cache.
        """
        uid = resource.metadata.uid
        version = resource.metadata.resource_version
        self.CACHED_RESOURCES[uid] = version
