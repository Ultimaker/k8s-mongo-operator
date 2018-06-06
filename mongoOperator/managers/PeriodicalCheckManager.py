# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging

from kubernetes.client import V1Service, V1Secret, V1beta1StatefulSet
from typing import Union

from kubernetes.client.rest import ApiException

from mongoOperator.managers.Manager import Manager
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService
from mongoOperator.services.MongoService import MongoService


class PeriodicalCheckManager(Manager):
    """
    Manager that periodically checks the status of the MongoDB objects in the cluster.
    """
    
    CACHED_RESOURCES = {}

    kubernetes_service = KubernetesService()
    mongo_service = MongoService(kubernetes_service)

    def execute(self) -> None:
        """Execute the manager logic."""
        self._checkExisting()
        self._collectGarbage()

    def _checkExisting(self) -> None:
        """
        Check all Mongo objects and see if the sub objects are available.
        If they are not, they should be (re-)created to ensure the cluster is in the expected state.
        """
        mongo_objects = self.kubernetes_service.listMongoObjects()
        logging.debug("Found %s mongo objects.", len(mongo_objects["items"]))
        for cluster_dict in mongo_objects["items"]:
            cluster_object = V1MongoClusterConfiguration(**cluster_dict)
            self._checkService(cluster_object)
            self._checkStatefulSet(cluster_object)
            self._checkOperatorAdminSecrets(cluster_object)
            self.mongo_service.checkReplicaSetNeedsSetup(cluster_object)

    def _checkService(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """Check and ensure the service is running."""
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        try:
            service = self.kubernetes_service.getService(name, namespace)
        except ApiException as e:
            service = None
            if e.status != 404:
                raise

        if not service:
            # The service does not exist but should, so we create it.
            service = self.kubernetes_service.createService(cluster_object)
        elif not self._isCachedResource(service):
            # If it's not cached, we update the service to ensure it is up to date.
            service = self.kubernetes_service.updateService(cluster_object)

        # Finally we cache the latest known version of the object.
        self._cacheResource(service)
        logging.info("Status of service %s @ %s is %s", name, namespace, service.status)

    def _checkOperatorAdminSecrets(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """Check and ensure the stateful set is running."""
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        try:
            secret = self.kubernetes_service.getOperatorAdminSecret(name, namespace)
        except ApiException as e:
            if e.status == 404:
                # The secret does not exist but it should, so we create it.
                secret = self.kubernetes_service.createOperatorAdminSecret(cluster_object)
            else:
                raise

        logging.info("Operator Admin Secret for %s @ %s has keys %s", name, namespace, sorted(secret.data.keys()))

    def _checkStatefulSet(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """Check and ensure the stateful set is running."""
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        try:
            stateful_set = self.kubernetes_service.getStatefulSet(name, namespace)
        except ApiException as e:
            stateful_set = None
            if e.status != 404:
                raise

        if not stateful_set:
            stateful_set = self.kubernetes_service.createStatefulSet(cluster_object)
        elif not self._isCachedResource(stateful_set):
            # If it's not cached, we update the stateful set to ensure it is up to date.
            stateful_set = self.kubernetes_service.updateStatefulSet(cluster_object)

        # Finally we cache the latest known version of the object.
        self._cacheResource(stateful_set)
        logging.info("Status of stateful set of service %s @ %s is %s with %s replicas", name, namespace,
                     stateful_set.status, stateful_set.spec.replicas)
    
    def _collectGarbage(self) -> None:
        """Collect garbage."""
        self._cleanServices()
        self._cleanStatefulSets()
        self._cleanSecrets()

    def _cleanServices(self) -> None:
        """Clean left-over services."""
        services = self.kubernetes_service.listAllServicesWithLabels()

        for service in services.items:
            name = service.metadata.name
            namespace = service.metadata.namespace
            try:
                self.kubernetes_service.getMongoObject(name, namespace)
            except ApiException as e:
                if e.status != 404:
                    raise
                # The service exists but the Mongo object it belonged to does not, we have to delete it.
                self.kubernetes_service.deleteService(name, namespace)

    def _cleanStatefulSets(self) -> None:
        """Clean left-over stateful sets."""
        stateful_sets = self.kubernetes_service.listAllStatefulSetsWithLabels()

        for stateful_set in stateful_sets.items:
            name = stateful_set.metadata.name
            namespace = stateful_set.metadata.namespace
            try:
                self.kubernetes_service.getMongoObject(name, namespace)
            except ApiException as e:
                if e .status != 404:
                    raise
                # The stateful set exists but the Mongo object is belonged to does not, we have to delete it.
                self.kubernetes_service.deleteStatefulSet(name, namespace)

    def _cleanSecrets(self) -> None:
        """Clean left-over secrets."""
        secrets = self.kubernetes_service.listAllSecretsWithLabels()

        for secret in secrets.items:
            name = secret.metadata.name
            namespace = secret.metadata.namespace
            try:
                self.kubernetes_service.getMongoObject(name, namespace)
            except ApiException as e:
                if e.status != 404:
                    raise
                # The secret exists but the Mongo object is belonged to does not, we have to delete it.
                self.kubernetes_service.deleteSecret(name, namespace)

    def _isCachedResource(self, resource: Union[V1Service, V1Secret, V1beta1StatefulSet]) -> bool:
        """
        Check if we have cached the specific version of the given resource locally.
        This limits the amount of calls to the Kubernetes API.
        :param resource: Kubernetes cluster resource to check cache for.
        :return: True if cached, False otherwise.
        """
        uid = resource.metadata.uid
        version = resource.metadata.resource_version
        return uid in self.CACHED_RESOURCES and self.CACHED_RESOURCES[uid] == version

    def _cacheResource(self, resource: Union[V1Service, V1Secret, V1beta1StatefulSet]) -> None:
        """
        Cache a resource by version locally.
        This limits the amount of calls to the Kubernetes API.
        :param resource: Kubernetes cluster resource to cache.
        """
        uid = resource.metadata.uid
        version = resource.metadata.resource_version
        self.CACHED_RESOURCES[uid] = version
