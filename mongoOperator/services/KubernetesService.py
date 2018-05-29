# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from typing import Dict, Optional

import yaml
from kubernetes import client
from kubernetes.client import Configuration
from kubernetes.client.rest import ApiException

from mongoOperator.Settings import Settings
from mongoOperator.helpers.KubernetesResources import KubernetesResources


class KubernetesService:
    """
    Bundled methods for interacting with the Kubernetes API.
    """
    
    # Easy definable secret formats.
    OPERATOR_ADMIN_SECRET_FORMAT = "{}-admin-credentials"
    
    def __init__(self):
        # Create Kubernetes config.
        kubernetes_config = Configuration()
        kubernetes_config.host = Settings.KUBERNETES_SERVICE_HOST
        kubernetes_config.verify_ssl = not Settings.KUBERNETES_SERVICE_SKIP_TLS
        kubernetes_config.debug = Settings.KUBERNETES_SERVICE_DEBUG
        api_client = client.ApiClient(configuration=kubernetes_config)
        
        # Re-usable API client instances.
        self.custom_objects_api = client.CustomObjectsApi(api_client)
        self.core_api = client.CoreV1Api(api_client)
        self.extensions_api = client.ApiextensionsV1beta1Api(api_client)

    def createMongoObjectDefinition(self) -> None:
        """Create the custom resource definition."""
        available_crds = [crd['spec']['names']['kind'].lower() for crd in
                          self.extensions_api.list_custom_resource_definition().to_dict()['items']]
        if Settings.CUSTOM_OBJECT_RESOURCE_NAME not in available_crds:
            # Create it if our CRD doesn't exists yet.
            logging.info("Custom resource definition {} not found in cluster, creating it...".format(
                    Settings.CUSTOM_OBJECT_RESOURCE_NAME))
            with open("../../mongo_crd.yaml") as data:
                body = yaml.load(data)
                self.extensions_api.create_custom_resource_definition(body)

    def listMongoObjects(self, **kwargs) -> list:
        """
        Get all Kubernetes objects of our custom resource type.
        :param kwargs: Additional API flags.
        :return: List of custom resource objects.
        """
        self.createMongoObjectDefinition()
        return self.custom_objects_api.list_cluster_custom_object(Settings.CUSTOM_OBJECT_API_NAMESPACE,
                                                                  Settings.CUSTOM_OBJECT_API_VERSION,
                                                                  Settings.CUSTOM_OBJECT_RESOURCE_NAME,
                                                                  **kwargs)

    def createOperatorAdminSecret(self, cluster_object) -> Optional[client.V1Secret]:
        """Create the operator admin secret."""
        secret_data = {"username": "root", "password": KubernetesResources.createRandomPassword()}
        return self.createSecret(self.OPERATOR_ADMIN_SECRET_FORMAT.format(cluster_object.metadata.name),
                                 cluster_object.metadata.namespace, secret_data)
    
    def updateOperatorAdminSecret(self, cluster_object) -> None:
        pass

    def deleteOperatorAdminSecret(self, cluster_object) -> bool:
        """Delete the operator admin secret."""
        return self.deleteSecret(self.OPERATOR_ADMIN_SECRET_FORMAT.format(cluster_object.metadata.name),
                                 cluster_object.metadata.namespace)
    
    def getSecret(self, secret_name: str, namespace: str) -> None:
        pass
    
    def createSecret(self, secret_name: str, namespace: str, secret_data: Dict[str, str]) -> Optional[client.V1Secret]:
        """
        Create a new Kubernetes secret.
        :param secret_name: Unique name of the secret.
        :param namespace: Namespace to add secret to.
        :param secret_data: The data to store in the secret as key/value pair dict.
        :return: success boolean.
        """
        if self.getSecret(secret_name, namespace):
            # Check if the secret already exists and prevent creating a new one if it does.
            return None
        
        try:
            # Create the secret object.
            secret_body = KubernetesResources.createSecret(secret_name, namespace, secret_data)
            secret = self.core_api.create_namespaced_secret(namespace, secret_body)
            logging.info("Created secret {} in namespace {}".format(secret_name, namespace))
            return secret
        except ApiException as error:
            if error.status == 409:
                # The secret already exists.
                logging.warning("Tried to create a secret that already existed: {} in namespace {}".format(secret_name,
                                                                                                           namespace))
            logging.exception(error)
            return None
    
    def deleteSecret(self, secret_name, namespace: str) -> bool:
        pass
    
    def createService(self, cluster_object) -> None:
        pass
    
    def updateService(self, cluster_object) -> None:
        pass
    
    def deleteService(self, cluster_object) -> bool:
        pass
    
    def createStatefulSet(self, cluster_object) -> None:
        pass
    
    def updateStatefulSet(self, cluster_object) -> None:
        pass
    
    def deleteStatefulSet(self, cluster_object) -> bool:
        pass
