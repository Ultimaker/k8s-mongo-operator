# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from base64 import b64decode
from typing import List

from kubernetes import client


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
    def createMongoExecCommand(cls, mongo_command: str) -> List[str]:
        """
        Creates a command that can be executed in Kubernetes (`kubectl exec`) that will execute the given mongo command.
        :param mongo_command: The command to be executed inside MongoDB.
        :return: A list of arguments to be passed to Kubernetes.
        """
        return [
            "mongo", "localhost:27017/admin",
            #TODO: use SSL with MongoDB.
            # "--ssl",
            # "--sslCAFile", "/etc/ssl/mongod/ca.pem",
            # "--sslPEMKeyFile", "/etc/ssl/mongod/mongod.pem",
            "--eval", mongo_command
        ]

    @classmethod
    def createReplicaInitiateCommand(cls, cluster_object) -> str:
        """
        Creates a MongoDB command that initiates the replica set, i.e. a rs.initiate() command with the host names.
        :param cluster_object: The cluster object from the YAML file.
        :return: The command to be sent to MongoDB.
        """
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        replicas = cluster_object.spec.mongodb.replicas
        replica_set_config = {
            "_id": name,
            "version": 1,
            "members": [{"_id": i, "host": cls.getMemberHostname(i, name, namespace)} for i in range(replicas)],
        }
        return "rs.initiate({})".format(json.dumps(replica_set_config))

    @classmethod
    def createCreateAdminCommand(cls, admin_credentials: client.V1Secret) -> str:
        """
        Creates a MongoDB command that creates administrator users.
        :param admin_credentials: The admin credentials secret model.
        :return: The command to be sent to MongoDB.
        """
        admin_username = b64decode(admin_credentials.data["username"]).decode("utf-8")
        admin_password = b64decode(admin_credentials.data["password"]).decode("utf-8")
        return '''
            admin = db.getSiblingDB("admin")
            admin.createUser({{
                user: "{user}", pwd: "{password}",
                roles: [ {{ role: "root", db: "admin" }} ]
            }})
            admin.auth("{user}", "{password}")
        '''.format(user=admin_username, password=admin_password)

    @classmethod
    def createStatusCommand(cls) -> str:
        """
        Returns the string that is used to retrieve the status from the MongoDB replica set.
        :return: The command to be sent to MongoDB.
        """
        return "rs.status()"
