# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from time import sleep
from unittest.mock import patch

from kubernetes.client.rest import ApiException
from typing import Dict, Optional

import yaml
from kubernetes.config import load_incluster_config
from kubernetes import client
from kubernetes.client import Configuration, V1DeleteOptions, V1ServiceList, V1StatefulSetList, V1SecretList, \
    V1beta1CustomResourceDefinition

from Settings import Settings
from mongoOperator.helpers.IgnoreIfExists import IgnoreIfExists
from mongoOperator.helpers.KubernetesResources import KubernetesResources
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration


class KubernetesService:
    """
    Bundled methods for interacting with the Kubernetes API.
    """

    DEFAULT_LABELS = KubernetesResources.createDefaultLabels()

    # after creating a new object definition we can get 404 not found from K8s.
    # below we can configure how many times we retry and how long we wait in between.
    LIST_CUSTOM_OBJECTS_RETRIES = 3
    LIST_CUSTOM_OBJECTS_WAIT = 5.0

    def __init__(self):
        # Create Kubernetes config.
        load_incluster_config()
        config = Configuration()
        config.debug = Settings.KUBERNETES_SERVICE_DEBUG
        self.api_client = client.ApiClient(config)

        # Re-usable API client instances.
        self.core_api = client.CoreV1Api(self.api_client)
        self.custom_objects_api = client.CustomObjectsApi(self.api_client)
        self.extensions_api = client.ApiextensionsV1beta1Api(self.api_client)
        self.apps_api = client.AppsV1beta1Api(self.api_client)

    def createMongoObjectDefinition(self) -> V1beta1CustomResourceDefinition:
        """Create the custom resource definition."""
        available_resources = {crd.spec.names.plural: crd for crd in
                               self.extensions_api.list_custom_resource_definition().items}
        if Settings.CUSTOM_OBJECT_RESOURCE_PLURAL in available_resources:
            return available_resources[Settings.CUSTOM_OBJECT_RESOURCE_PLURAL]

        # Create it if our CRD doesn't exists yet.
        logging.info("Custom resource definition %s not found in cluster (available: %s), creating it...",
                     Settings.CUSTOM_OBJECT_RESOURCE_PLURAL, available_resources)
        with open("mongo_crd.yaml") as f:
            definition_dict = yaml.load(f)
        body = KubernetesResources.deserialize(definition_dict, "V1beta1CustomResourceDefinition")

        # issue with kubernetes causes status.condition==null, which raises an exception and breaks the connection.
        # by ignoring the validation of this field in the client, we can keep the connection open.
        with patch("kubernetes.client.models.v1beta1_custom_resource_definition_status.V1beta1CustomResourceDefinitionStatus.conditions"):
            return self.extensions_api.create_custom_resource_definition(body)

    def listMongoObjects(self, **kwargs) -> Dict[str, any]:
        """
        Get all Kubernetes objects of our custom resource type.
        IMPORTANT: Kubernetes uses the :return: value to deserialize the results, so it must be a class name.
        :param kwargs: Additional API flags.
        :return: dict(str, object)
        """
        definition = self.createMongoObjectDefinition()
        for _ in range(self.LIST_CUSTOM_OBJECTS_RETRIES):
            try:
                logging.debug("Listing resources based on definition %s", definition.metadata.uid)
                return self.custom_objects_api.list_cluster_custom_object(Settings.CUSTOM_OBJECT_API_GROUP,
                                                                          Settings.CUSTOM_OBJECT_API_VERSION,
                                                                          Settings.CUSTOM_OBJECT_RESOURCE_PLURAL,
                                                                          **kwargs)
            except ApiException as e:
                if e.status != 404:
                    raise
                logging.info("Could not list the custom Mongo objects: %s. The definition is probably being "
                             "initialized, we wait %s seconds.", e.reason, self.LIST_CUSTOM_OBJECTS_WAIT)
                sleep(self.LIST_CUSTOM_OBJECTS_WAIT)

        raise TimeoutError("Could not list the custom mongo objects after {} retries"
                           .format(self.LIST_CUSTOM_OBJECTS_RETRIES))

    def getMongoObject(self, name: str, namespace: str) -> V1MongoClusterConfiguration:
        """
        Get a single Kubernetes Mongo object.
        :param name: The name of the object to get.
        :param namespace: The namespace in which to get the object.
        :return: The custom resource object if existing, otherwise None
        """
        return self.custom_objects_api.get_namespaced_custom_object(Settings.CUSTOM_OBJECT_API_GROUP,
                                                                    Settings.CUSTOM_OBJECT_API_VERSION,
                                                                    namespace,
                                                                    Settings.CUSTOM_OBJECT_RESOURCE_PLURAL,
                                                                    name)

    def listAllServicesWithLabels(self, labels: Dict[str, str] = DEFAULT_LABELS) -> V1ServiceList:
        """Get all services with the given labels."""
        label_selector = KubernetesResources.createLabelSelector(labels)
        logging.debug("Getting all services with labels %s", label_selector)
        return self.core_api.list_service_for_all_namespaces(label_selector=label_selector)

    def listAllStatefulSetsWithLabels(self, labels: Dict[str, str] = DEFAULT_LABELS) -> V1StatefulSetList:
        """Get all stateful sets with the given labels."""
        label_selector = KubernetesResources.createLabelSelector(labels)
        logging.debug("Getting all stateful sets with labels %s", label_selector)
        return self.apps_api.list_stateful_set_for_all_namespaces(label_selector=label_selector)

    def listAllSecretsWithLabels(self, labels: Dict[str, str] = DEFAULT_LABELS) -> V1SecretList:
        """Get al secrets with the given labels."""
        label_selector = KubernetesResources.createLabelSelector(labels)
        logging.debug("Getting all secrets with labels %s", label_selector)
        return self.core_api.list_secret_for_all_namespaces(label_selector=label_selector)

    def getSecret(self, secret_name: str, namespace: str) -> client.V1Secret:
        """
        Retrieves the secret with the given name.
        :param secret_name: The name of the secret.
        :param namespace: The namespace of the secret.
        :return: The secret object.
        """
        return self.core_api.read_namespaced_secret(secret_name, namespace)

    def createSecret(self, secret_name: str, namespace: str, secret_data: Dict[str, str],
                     labels: Optional[Dict[str, str]] = None) -> Optional[client.V1Secret]:
        """
        Creates a new Kubernetes secret.
        :param secret_name: Unique name of the secret.
        :param namespace: Namespace to add secret to.
        :param secret_data: The data to store in the secret as key/value pair dict.
        :param labels: Optional labels for this secret, defaults to the default labels (see `cls.createDefaultLabels`).
        :return: The secret if successful, None otherwise.
        """
        secret_body = KubernetesResources.createSecret(secret_name, namespace, secret_data, labels)
        logging.info("Creating secret %s in namespace %s", secret_name, namespace)
        with IgnoreIfExists():
            return self.core_api.create_namespaced_secret(namespace, secret_body)

    def updateSecret(self, secret_name: str, namespace: str, secret_data: Dict[str, str]) -> client.V1Secret:
        """
        Updates the given Kubernetes secret.
        :param secret_name: Unique name of the secret.
        :param namespace: Namespace to add secret to.
        :param secret_data: The data to store in the secret as key/value pair dict.
        :return: The secret if successful, None otherwise.
        """
        secret = self.getSecret(secret_name, namespace)
        secret.string_data = secret_data
        logging.info("Updating secret %s @ ns/%s", secret_name, namespace)
        return self.core_api.patch_namespaced_secret(secret_name, namespace, secret)

    def deleteSecret(self, name: str, namespace: str) -> client.V1Status:
        """
        Deletes the given Kubernetes secret.
        :param name: Name of the secret to delete.
        :param namespace: Namespace in which to delete the secret.
        :return: The deletion status.
        """
        body = V1DeleteOptions()
        logging.info("Deleting secret %s @ ns/%s.", name, namespace)
        return self.core_api.delete_namespaced_secret(name, namespace, body)

    def getService(self, name: str, namespace: str) -> client.V1Service:
        """
        Gets an existing service from the cluster.
        :param name: The name of the service to get.
        :param namespace: The namespace in which to get the service.
        :return: The service object if it exists, otherwise None.
        """
        return self.core_api.read_namespaced_service(name, namespace)

    def createService(self, cluster_object: V1MongoClusterConfiguration) -> Optional[client.V1Service]:
        """
        Creates the given cluster.
        :param cluster_object: The cluster object from the YAML file.
        :return: The created service.
        """
        namespace = cluster_object.metadata.namespace
        body = KubernetesResources.createService(cluster_object)
        logging.info("Creating service %s @ ns/%s.", body.metadata.name, namespace)
        with IgnoreIfExists():
            return self.core_api.create_namespaced_service(namespace, body)

    def updateService(self, cluster_object: V1MongoClusterConfiguration) -> client.V1Service:
        """
        Updates the given cluster.
        :param cluster_object: The cluster object from the YAML file.
        :return: The updated service.
        """
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        body = KubernetesResources.createService(cluster_object)
        logging.info("Updating service %s @ ns/%s.", name, namespace)
        return self.core_api.patch_namespaced_service(name, namespace, body)

    def deleteService(self, name: str, namespace: str) -> client.V1Status:
        """
        Deletes the service with the given name.
        :param name: The name of the service to delete.
        :param namespace: The namespace in which to delete the service.
        :return: The deletion status.
        """
        logging.info("Deleting service %s @ ns/%s.", name, namespace)
        body = V1DeleteOptions()
        try:
            return self.core_api.delete_namespaced_service(name, namespace, body)
        except TypeError:
            # bug in kubernetes client 5.0.0 - body parameter was missing.
            return self.core_api.delete_namespaced_service(name, namespace)

    def getStatefulSet(self, name: str, namespace: str) -> client.V1beta1StatefulSet:
        """
        Get an existing stateful set from the cluster.
        :param name: The name of the stateful set to get.
        :param namespace: The namespace in which to get the stateful set.
        :return: The stateful set object if existing, otherwise None.
        """
        return self.apps_api.read_namespaced_stateful_set(name, namespace)

    def createStatefulSet(self, cluster_object: V1MongoClusterConfiguration) -> Optional[client.V1beta1StatefulSet]:
        """
        Creates the stateful set for the given cluster object.
        :param cluster_object: The cluster object from the YAML file.
        :return: The created stateful set.
        """
        namespace = cluster_object.metadata.namespace
        body = KubernetesResources.createStatefulSet(cluster_object)
        with IgnoreIfExists():
            logging.info("Creating stateful set %s @ ns/%s.", body.metadata.name, namespace)
            return self.apps_api.create_namespaced_stateful_set(namespace, body)

    def updateStatefulSet(self, cluster_object: V1MongoClusterConfiguration) -> client.V1beta1StatefulSet:
        """
        Updates the stateful set for the given cluster object.
        :param cluster_object: The cluster object from the YAML file.
        :return: The updated stateful set.
        """
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        body = KubernetesResources.createStatefulSet(cluster_object)
        logging.info("Updating stateful set %s @ ns/%s.", name, namespace)
        return self.apps_api.patch_namespaced_stateful_set(name, namespace, body)

    def deleteStatefulSet(self, name: str, namespace: str) -> bool:
        """
        Deletes the stateful set for the given cluster object.
        :param name: The name of the stateful set to delete.
        :param namespace: The namespace in which to delete the stateful set.
        :return: The updated stateful set.
        """
        body = V1DeleteOptions()
        logging.info("Deleting stateful set %s @ ns/%s.", name, namespace)
        return self.apps_api.delete_namespaced_stateful_set(name, namespace, body)
