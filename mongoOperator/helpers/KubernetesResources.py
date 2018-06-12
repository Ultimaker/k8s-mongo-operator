# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from copy import deepcopy

import uuid
from kubernetes.client import V1beta1CustomResourceDefinition, V1ObjectMeta, V1beta1CustomResourceDefinitionSpec, \
    V1beta1CustomResourceDefinitionNames, models as k8s_models
from typing import Dict

from kubernetes import client

from Settings import Settings
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration


class KubernetesResources:
    """
    Helper class responsible for creating the Kubernetes model objects.
    """

    MONGO_IMAGE = "mongo:3.6.4"
    MONGO_NAME = "mongodb"
    MONGO_PORT = 27017
    MONGO_COMMAND = "mongod --replSet {name} --bind_ip 0.0.0.0 --smallfiles --noprealloc"
    MONGO_STORAGE_NAME = "mongo-storage"

    STORAGE_SIZE = "30Gi"
    STORAGE_MOUNT_PATH = "/data/db"

    DEFAULT_CPU_LIMIT = "100m"
    DEFAULT_MEMORY_LIMIT = "64Mi"

    @staticmethod
    def createRandomPassword() -> str:
        """Generate a random secure password to use in secrets."""
        return uuid.uuid4().hex

    @classmethod
    def createSecret(cls, secret_name: str, namespace: str, secret_data: Dict[str, str]) -> client.V1Secret:
        """
        Creates a secret object.
        :param secret_name: The name of the secret.
        :param namespace: The name space for the secret.
        :param secret_data: The secret data.
        :return: The secret model object.
        """
        return client.V1Secret(
            metadata=client.V1ObjectMeta(
                name=secret_name,
                namespace=namespace,
                labels=cls.createDefaultLabels(secret_name)
            ),
            string_data=secret_data,
        )

    @staticmethod
    def createDefaultLabels(name: str = None) -> Dict[str, str]:
        """
        Creates the labels for the object with the given name.
        :param name: The name of the object.
        :return: The object's metadata dictionary.
        """
        return {
            "operated-by": Settings.CUSTOM_OBJECT_API_GROUP,
            "heritage": Settings.CUSTOM_OBJECT_RESOURCE_PLURAL,
            "name": name if name else ""
        }

    @classmethod
    def createService(cls, cluster_object: V1MongoClusterConfiguration) -> client.V1Service:
        """
        Creates a service model object.
        :param cluster_object: The cluster object from the YAML file.
        :return: The service object.
        """
        # Parse cluster data object.
        name = cluster_object.metadata.name

        # Create service.
        return client.V1Service(
            metadata=client.V1ObjectMeta(
                name=name,
                namespace=cluster_object.metadata.namespace,
                labels=cls.createDefaultLabels(name),
            ),
            spec=client.V1ServiceSpec(
                cluster_ip="None",  # create headless service, no load-balancing and a single service IP
                selector=cls.createDefaultLabels(name),
                ports=[client.V1ServicePort(
                    name="mongod",
                    port=cls.MONGO_PORT,
                    protocol="TCP"
                )],
            ),
        )

    @classmethod
    def createStatefulSet(cls, cluster_object: V1MongoClusterConfiguration) -> client.V1beta1StatefulSet:
        """
        Creates a the stateful set configuration for the given cluster.
        :param cluster_object: The cluster object from the YAML file.
        :return: The stateful set object.
        """

        # Parse cluster data object.
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        replicas = cluster_object.spec.mongodb.replicas
        cpu_limit = cluster_object.spec.mongodb.cpu_limit or cls.DEFAULT_CPU_LIMIT
        memory_limit = cluster_object.spec.mongodb.memory_limit or cls.DEFAULT_MEMORY_LIMIT

        # create container
        mongo_container = client.V1Container(
            name=cls.MONGO_NAME,
            env=[client.V1EnvVar(
                name="POD_IP",
                value_from=client.V1EnvVarSource(
                    field_ref = client.V1ObjectFieldSelector(
                        api_version = "v1",
                        field_path = "status.podIP"
                    )
                )
            )],
            command=cls.MONGO_COMMAND.format(name=name).split(),
            image=cls.MONGO_IMAGE,
            ports=[client.V1ContainerPort(
                name=cls.MONGO_NAME,
                container_port=cls.MONGO_PORT,
                protocol="TCP"
            )],
            volume_mounts=[client.V1VolumeMount(
                name=cls.MONGO_STORAGE_NAME,
                read_only=False,
                mount_path=cls.STORAGE_MOUNT_PATH
            )],
            resources=client.V1ResourceRequirements(
                limits={"cpu": cpu_limit, "memory": memory_limit},
                requests={"cpu": cpu_limit, "memory": memory_limit}
            )
        )

        # Create stateful set.
        return client.V1beta1StatefulSet(
            metadata = client.V1ObjectMeta(name=name, namespace=namespace, labels=cls.createDefaultLabels(name)),
            spec = client.V1beta1StatefulSetSpec(
                replicas = replicas,
                service_name = name,
                template = client.V1PodTemplateSpec(
                    metadata = client.V1ObjectMeta(labels=cls.createDefaultLabels(name)),
                    spec = client.V1PodSpec(containers=[mongo_container]),
                ),
                volume_claim_templates = [client.V1PersistentVolumeClaim(
                    metadata = client.V1ObjectMeta(name=cls.MONGO_STORAGE_NAME),
                    spec = client.V1PersistentVolumeClaimSpec(
                        access_modes = ["ReadWriteOnce"],
                        resources = client.V1ResourceRequirements(requests={"storage": cls.STORAGE_SIZE}),
                    ),
                )],
            ),
        )

    @classmethod
    def createLabelSelector(cls, labels: Dict[str, str]) -> str:
        """
        Converts the given label dictionary into a label selector string.
        :param labels: The labels dict, e.g. {"name": "test"}.
        :return: The label selector, e.g. "name=test".
        """
        return ",".join("{}={}".format(k, v) for k, v in labels.items() if v)

    @classmethod
    def deserialize(cls, data: dict, model_name: str) -> any:
        """
        Deserializes the dictionary into a kubernetes model.
        :param data: The data dictionary.
        :param model_name: The name of the model.
        :return: An instance of the model with the given name.
        """
        model_class = getattr(k8s_models, model_name, None)
        if not model_class or not isinstance(data, dict):
            return data
        kwargs = {}
        if model_class.swagger_types is not None:
            for attr, attr_type in model_class.swagger_types.items():
                if model_class.attribute_map.get(attr):
                    value = data.get(model_class.attribute_map[attr])
                    kwargs[attr] = cls.deserialize(value, attr_type)

        return model_class(**kwargs)
