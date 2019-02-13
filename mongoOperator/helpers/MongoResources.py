# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
from json import JSONDecodeError

import re
from base64 import b64decode
from typing import List, Dict, Tuple, Any, Union

from kubernetes import client

from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration


class MongoResources:
    """
    Helper class responsible for creating the Mongo commands.
    """

    @classmethod
    def getMemberHostname(cls, pod_index, cluster_name, namespace) -> str:
        """
        Creates the string that is used as hostname for the pods.
        :param pod_index: The index of the pod.
        :param cluster_name: The name of the cluster.
        :param namespace: The namespace of the cluster.
        :return: The name of the host.
        """
        return "{}-{}.{}.{}.svc.cluster.local".format(cluster_name, pod_index, cluster_name, namespace)

    @classmethod
    def createReplicaInitiateCommand(cls, cluster_object) -> Tuple[str, dict]:
        """
        Creates a MongoDB command that initiates the replica set, i.e. a rs.initiate() command with the host names.
        :param cluster_object: The cluster object from the YAML file.
        :return: The command to be sent to MongoDB.
        """
        replica_set_config = cls._createReplicaConfig(cluster_object)
        return "replSetInitiate", replica_set_config

    @classmethod
    def createReplicaReconfigureCommand(cls, cluster_object) -> Tuple[str, dict]:
        """
        Creates a MongoDB command that reconfigures the replica set, i.e. a rs.reconfig() command with the host names.
        :param cluster_object: The cluster object from the YAML file.
        :return: The command to be sent to MongoDB.
        """
        replica_set_config = cls._createReplicaConfig(cluster_object)
        return "replSetReconfig", replica_set_config

    @classmethod
    def createCreateAdminCommand(cls, admin_credentials: client.V1Secret)\
            -> Tuple[str, Any, Dict[str, Union[List[Dict[str, str]], Any]]]:
        """
        Creates a MongoDB command that creates administrator users.
        :param admin_credentials: The admin credentials secret model.
        :return: The command to be sent to MongoDB.
        """
        admin_username = b64decode(admin_credentials.data["username"]).decode("utf-8")
        admin_password = b64decode(admin_credentials.data["password"]).decode("utf-8")
        kwargs = {
            "pwd": admin_password,
            "roles": [
                {"role": "root", "db": "admin"}
            ]
        }
        return "createUser", admin_username, kwargs

    @classmethod
    def createStatusCommand(cls) -> str:
        """
        Returns the string that is used to retrieve the status from the MongoDB replica set.
        :return: The command to be sent to MongoDB.
        """
        return "replSetGetStatus"

    @classmethod
    def _createReplicaConfig(cls, cluster_object: V1MongoClusterConfiguration) -> Dict[str, any]:
        """
        Creates a dict with the replica set configuration for mongo.
        :param cluster_object: The cluster object from the YAML file.
        :return: A dict with the configuration.
        """
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        replicas = cluster_object.spec.mongodb.replicas
        return {
            "_id": name,
            "version": 1,
            "members": [{"_id": i, "host": cls.getMemberHostname(i, name, namespace)} for i in range(replicas)],
        }

    @classmethod
    def getConnectionSeeds(cls, cluster_object: V1MongoClusterConfiguration) -> List[str]:
        """
        Creates a list with the replica set members for mongo.
        :param cluster_object: The cluster object from the YAML file.
        :return: A list with the member hostnames.
        """
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        replicas = cluster_object.spec.mongodb.replicas
        return [cls.getMemberHostname(i, name, namespace) for i in range(replicas)]
