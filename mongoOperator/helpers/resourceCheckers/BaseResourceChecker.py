# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from abc import abstractmethod

from kubernetes.client import V1Status
from kubernetes.client.rest import ApiException
from typing import TypeVar, List

from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService

GenericType = TypeVar("GenericType")


class BaseResourceChecker:
    """
    Base class for services that can check Kubernetes resources.
    """

    def __init__(self, kubernetes_service: KubernetesService):
        self.kubernetes_service = kubernetes_service

    @staticmethod
    def getClusterName(resource_name: str) -> str:
        """
        Gets the cluster name based on the name of the resource. By default both names are the same.
        :param resource_name: The name of the resource.
        :return: The name of the cluster.
        """
        return resource_name

    def checkResource(self, cluster_object: V1MongoClusterConfiguration) -> GenericType:
        """
        Checks whether the resource is up-to-date in Kubernetes, creating or updating it if necessary.
        :param cluster_object: The cluster object from the YAML file.
        :return: An instance of the resource.
        """
        try:
            resource = self.getResource(cluster_object)
        except ApiException as api_exception:
            resource = None
            if api_exception.status != 404:
                raise

        if resource:
            # We update the resource to ensure it is up to date.
            resource = self.updateResource(cluster_object)
        else:
            # The resource does not exist but should, so we create it.
            resource = self.createResource(cluster_object)

        # Finally we cache the latest known version of the object.
        logging.info("%s for %s @ ns/%s reports version %s", type(self).__name__, cluster_object.metadata.name,
                     cluster_object.metadata.namespace, resource.metadata.resource_version)
        return resource

    def cleanResources(self) -> None:
        """
        Deletes any resources for which the original cluster cannot be found.
        """
        for resource in self.listResources():
            resource_name = resource.metadata.name
            cluster_name = self.getClusterName(resource_name)
            namespace = resource.metadata.namespace
            try:
                self.kubernetes_service.getMongoObject(cluster_name, namespace)
                continue
            except ApiException as api_exception:
                if api_exception.status != 404:
                    raise

            # The service exists but the Mongo object it belonged to does not, we have to delete it.
            self.deleteResource(cluster_name, namespace)

    @abstractmethod
    def listResources(self) -> List[GenericType]:
        """
        Retrieves a list of resource objects.
        :return: The list of available resources.
        """
        raise NotImplementedError

    @abstractmethod
    def getResource(self, cluster_object: V1MongoClusterConfiguration) -> GenericType:
        """
        Retrieves the resource for the given cluster.
        :param cluster_object: The cluster object from the YAML file.
        :return: An instance of the resource.
        :raise ApiException(404): If the resource did not exist.
        """
        raise NotImplementedError

    @abstractmethod
    def createResource(self, cluster_object: V1MongoClusterConfiguration) -> GenericType:
        """
        Creates a new resource instance.
        :param cluster_object: The cluster object from the YAML file.
        :return: An instance of the resource.
        """
        raise NotImplementedError

    @abstractmethod
    def updateResource(self, cluster_object: V1MongoClusterConfiguration) -> GenericType:
        """
        Updates the given resource instance.
        :param cluster_object: The cluster object from the YAML file.
        :return: An instance of the resource.
        """
        raise NotImplementedError

    @abstractmethod
    def deleteResource(self, cluster_name: str, namespace: str) -> V1Status:
        """
        Deletes the resource for the given cluster.
        :param cluster_name: The name of the cluster.
        :param namespace: The cluster's namespace.
        :return: The deletion status.
        """
        raise NotImplementedError
