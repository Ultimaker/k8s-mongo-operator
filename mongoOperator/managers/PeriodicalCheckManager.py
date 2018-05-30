# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from kubernetes import client
from kubernetes.client.rest import ApiException

from mongoOperator.managers.Manager import Manager
from mongoOperator.services.KubernetesService import KubernetesService
from mongoOperator.services.MongoService import MongoService


class PeriodicalCheckManager(Manager):
    """
    Manager that periodically checks the status of the MongoDB objects in the cluster.
    """
    
    CACHED_RESOURCES = {}

    kubernetes_service = KubernetesService()
    mongo_service = MongoService()
    
    def _execute(self) -> None:
        """Execute the manager logic."""
        self._checkExisting()
        self._collectGarbage()
        
    def _beforeShuttingDown(self) -> None:
        """Abstract method we don't need in this manager."""
        pass

    def _checkExisting(self) -> None:
        """
        Check all Mongo objects and see if the sub objects are available.
        If they are not, they should be (re-)created to ensure the cluster is in the expected state.
        """
        mongo_objects = self.kubernetes_service.listMongoObjects()
        for cluster_object in mongo_objects:
            self._checkService(cluster_object)
            self._checkStatefulSet(cluster_object)
            self.mongo_service.checkReplicaSetNeedsSetup(cluster_object)

    def _checkService(self, cluster_object: "client.V1beta1CustomResourceDefinition") -> None:
        """Check and ensure the service is running."""
        name = cluster_object.metadata["name"]
        namespace = cluster_object.metadata["namespace"]
        try:
            service = self.kubernetes_service.getService(name, namespace)
        except ApiException as e:
            if e.status == 404:
                # The service does not exist but should, so we create it.
                service = self.kubernetes_service.createService(cluster_object)
                if service:
                    # We just created it, so we can cache it right away.
                    self._cacheResource(service)
            else:
                logging.exception(e)
                return

        if not self._isCachedResource(service):
            # If it's not cached, we update the service to ensure it is up to date.
            service = self.kubernetes_service.updateService(cluster_object)

        # Finally we cache the latest known version of the object.
        self._cacheResource(service)

    def _checkStatefulSet(self, cluster_object) -> None:
        """Check and ensure the stateful set is running."""
        name = cluster_object.metadata["name"]
        namespace = cluster_object.metadata["namespace"]
        try:
            stateful_set = self.kubernetes_service.getStatefulSet(name, namespace)
        except ApiException as e:
            if e.status == 404:
                # The stateful set does not exist but it should, so we create it.
                stateful_set = self.kubernetes_service.createStatefulSet(cluster_object)
                if stateful_set:
                    # We just created it, so we can cache it right away.
                    self._cacheResource(stateful_set)
            else:
                logging.exception(e)
                return

        if not self._isCachedResource(stateful_set):
            # If it's not cached, we update the stateful set to ensure it is up to date.
            stateful_set = self.kubernetes_service.updateStatefulSet(cluster_object)

        # Finally we cache the latest known version of the object.
        self._cacheResource(stateful_set)
    
    def _collectGarbage(self) -> None:
        """Collect garbage."""
        self._cleanServices()
        self._cleanStatefulSets()
        self._cleanSecrets()

    def _cleanServices(self) -> None:
        """Clean left-over services."""
        try:
            services = self.kubernetes_service.listAllServicesWithLabels()
        except ApiException as e:
            logging.exception(e)
            return

        for service in services:
            name = service.metadata["name"]
            namespace = service.metadata["namespace"]
            try:
                self.kubernetes_service.getMongoObject(name, namespace)
            except ApiException as e:
                if e.status == 404:
                    # The service exists but the Mongo object is belonged to does not, we have to delete it.
                    self.kubernetes_service.deleteService(name, namespace)
                else:
                    logging.exception(e)

    def _cleanStatefulSets(self) -> None:
        """Clean left-over stateful sets."""
        try:
            stateful_sets = self.kubernetes_service.listAllStatefulSetsWithLabels()
        except ApiException as e:
            logging.exception(e)
            return

        for stateful_set in stateful_sets:
            name = stateful_set.metadata["name"]
            namespace = stateful_set.metadata["namespace"]
            try:
                self.kubernetes_service.getMongoObject(name, namespace)
            except ApiException as e:
                if e .status == 404:
                    # The stateful set exists but the Mongo object is belonged to does not, we have to delete it.
                    self.kubernetes_service.deleteStatefulSet(name, namespace)
                else:
                    logging.exception(e)

    def _cleanSecrets(self) -> None:
        """Clean left-over secrets."""
        try:
            secrets = self.kubernetes_service.listAllSecretsWithLabels()
        except ApiException as e:
            logging.exception(e)
            return

        for secret in secrets:
            name = secret.metadata["name"]
            namespace = secret.metadata["namespace"]
            try:
                self.kubernetes_service.getMongoObject(name, namespace)
            except ApiException as e:
                if e.status == 404:
                    # The secret exists but the Mongo object is belonged to does not, we have to delete it.
                    self.kubernetes_service.deleteSecret(name, namespace)
                else:
                    logging.exception(e)

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
