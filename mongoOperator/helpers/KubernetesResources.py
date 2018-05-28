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

        # Fixed values (for now).
        mongo_image = "mongo:3.6.4"
        mongo_name = "mongodb"
        mongo_port = 27017
        mongo_command = ["mongod", "--replSet", name, "--bind_ip", "0.0.0.0", "--smallfiles", "--noprealloc"]
        storage_name = "mongo-storage"
        storage_size = "30Gi"
        storage_mount_path = "/data/db"
        
        # Create stateful set.
        stateful_set = client.V1beta1StatefulSet()
        
        stateful_set.metadata = client.V1ObjectMeta(
            name=name,
            namespace=namespace,
            labels=cls.createDefaultLabels(name)
        )
        
        stateful_set.spec = client.V1beta1StatefulSetSpec(
            replicas=replicas,
            service_name=name,
            template=client.V1PodTemplateSpec()
        )
        
        stateful_set.spec.template.metadata = client.V1ObjectMeta(
            labels=cls.createDefaultLabels(name)
        )
        
        # Create Mongo container.
        container_port = client.V1ContainerPort(
            name=mongo_name,
            container_port=mongo_port,
            protocol="TCP"
        )
        
        data_volume_mount = client.V1VolumeMount(
            name=storage_name,
            read_only=False,
            mount_path=storage_mount_path
        )
        
        resource_requirements = client.V1ResourceRequirements(
            limits={"cpu": cpu_limit, "memory": memory_limit},
            requests={"cpu": cpu_limit, "memory": memory_limit}
        )

        mongo_container = client.V1Container(
            name=mongo_name,
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
            command=mongo_command,
            image=mongo_image,
            ports=container_port,
            volume_mounts=[
                data_volume_mount
            ],
            resources=resource_requirements
        )

        stateful_set.spec.template.spec.containers = [mongo_container]

        # Create persistent volume claim.
        persistent_volume = client.V1PersistentVolumeClaim(
            metadata=client.V1ObjectMeta(
                name=storage_name
            ),
            spec=client.V1PersistentVolumeClaimSpec(
                access_modes=["ReadWriteOnce"],
                resources=client.V1ResourceRequirements(
                    requests={"storage": storage_size}
                )
            )
        )

        stateful_set.spec.volumeClaimTemplates = [persistent_volume]

        return stateful_set
