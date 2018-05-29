# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import uuid
from typing import Dict

from kubernetes import client

from mongoOperator.Settings import Settings


class KubernetesResources:
    
    @staticmethod
    def createRandomPassword() -> str:
        """Generate a random secure password to use in secrets."""
        return uuid.uuid4().hex
    
    @classmethod
    def createSecret(cls, secret_name: str, namespace: str, secret_data: Dict[str, str]) -> "client.V1Secret":
        secret = client.V1Secret()
        secret.metadata = client.V1ObjectMeta(
            name = secret_name,
            namespace = namespace,
            labels = cls.createDefaultLabels(secret_name)
        )
        secret.string_data = secret_data
        return secret

    @staticmethod
    def createDefaultLabels(name: str) -> Dict[str, str]:
        return {
            "operated-by": Settings.CUSTOM_OBJECT_API_NAMESPACE,
            "heritage": Settings.CUSTOM_OBJECT_RESOURCE_NAME,
            "name": name
        }

    @classmethod
    def createService(cls, cluster_object) -> "client.V1Service":
        
        # Parse cluster data object.
        name = cluster_object['metadata']['name']
        namespace = cluster_object['metadata']['namespace']
        
        # Create service.
        service = client.V1Service()
        
        service.metadata = client.V1ObjectMeta(
            name=name,
            namespace=namespace,
            labels = cls.createDefaultLabels(name)
        )
        
        mongodb_port = client.V1ServicePort(
            name="mongod",
            port=27017,
            protocol="TCP"
        )

        service.spec = client.V1ServiceSpec(
            cluster_ip="None",
            selector=cls.createDefaultLabels(name),
            ports=[mongodb_port]
        )
        
        return service

    @classmethod
    def createStatefulSet(cls, cluster_object) -> "client.V1beta1StatefulSet":
        
        # Parse cluster data object.
        name = cluster_object['metadata']['name']
        namespace = cluster_object['metadata']['namespace']
        replicas = cluster_object['spec']['mongodb']['replicas']
        cpu_limit = cluster_object['spec']['mongodb']['cpu_limit']
        memory_limit = cluster_object['spec']['mongodb']['memory_limit']
        
        # Create stateful set.
        return client.V1beta1StatefulSet(
            metadata=client.V1ObjectMeta(
                name=name,
                namespace=namespace,
                labels=cls.createDefaultLabels(name)
            ),
            spec=client.V1beta1StatefulSetSpec(
                replicas=replicas,
                service_name=name,
                template=client.V1PodTemplateSpec(
                    metadata=client.V1ObjectMeta(
                        labels=cls.createDefaultLabels(name)
                    ),
                    spec=client.V1PodSpec(
                        containers=[client.V1Container(
                            name="mongod",
                            env=[
                                client.V1EnvVar(
                                    name="POD_IP",
                                    value_from=client.V1EnvVarSource(
                                        field_ref = client.V1ObjectFieldSelector(
                                            api_version = "v1",
                                            field_path = "status.podIP"
                                        )
                                    )
                                )
                            ],
                            command=["mongod", "--replSet", name, "--bind_ip", "0.0.0.0",
                                     "--smallfiles", "--noprealloc"],
                            image="mongo:3.6.4",
                            ports=client.V1ContainerPort(
                                name="mongodb",
                                container_port=27017,
                                protocol="TCP"
                            ),
                            volume_mounts=[
                                client.V1VolumeMount(
                                    name="mongodb-data",
                                    read_only=False,
                                    mount_path="/data/db"
                                )
                            ],
                            resources=client.V1ResourceRequirements(
                                limits={"cpu": cpu_limit, "memory": memory_limit},
                                requests={"cpu": cpu_limit, "memory": memory_limit}
                            )
                        )],
                        volumes=[client.V1Volume(
                            name="mongo-data",
                            empty_dir=client.V1EmptyDirVolumeSource()
                        )],
                    ),
                ),
            ),
        )

    @classmethod
    def updateService(cls, cluster_object) -> client.V1Service:
        """
        Updates the given cluster.
        :param cluster_object: The cluster object from the YAML file.
        :return: The updated service.
        """
        name = cluster_object['metadata']['name']
        namespace = cluster_object['metadata']['namespace']
        v1 = client.CoreV1Api()
        body = cls.createService(cluster_object)
        return v1.patch_namespaced_service(name, namespace, body)

    @classmethod
    def deleteService(cls, name, namespace) -> client.V1Status:
        """
        Deletes the service with the given name.
        :param name: The name of the service.
        :param namespace: The namespace of the service.
        :return: The deletion status.
        """
        v1 = client.CoreV1Api()
        body = client.V1DeleteOptions()
        return v1.delete_namespaced_service(name, namespace, body)
