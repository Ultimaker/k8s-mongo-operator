# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
from json import JSONDecodeError

import re
from base64 import b64decode
from typing import List, Dict

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
        replica_set_config = cls._createReplicaConfig(cluster_object)
        return "rs.initiate({})".format(json.dumps(replica_set_config))

    @classmethod
    def createReplicaReconfigureCommand(cls, cluster_object) -> str:
        """
        Creates a MongoDB command that reconfigures the replica set, i.e. a rs.reconfig() command with the host names.
        :param cluster_object: The cluster object from the YAML file.
        :return: The command to be sent to MongoDB.
        """
        replica_set_config = cls._createReplicaConfig(cluster_object)
        return "rs.reconfig({})".format(json.dumps(replica_set_config))

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

    @classmethod
    def parseMongoResponse(cls, exec_response: str) -> Dict[str, any]:
        """
        Parses a response from the MongoDB daemon. See `tests/fixtures/mongo_responses` for some examples.
        :param exec_response: The response from Mongo.
        :return: The JSON object found in the response.
        :raise ValueError: If no JSON object was found.
        """
        json_search = re.search(r"^[^{}]+({[\s\S]*})[\s\d]+$", exec_response)
        if json_search:
            clean_json = json_search.group(1)
            clean_json = re.sub("Timestamp\((\d+), (\d)\)", r"\1.\2", clean_json)
            clean_json = re.sub("BinData\(0,(.+)\)", r"\1", clean_json)
            clean_json = re.sub("NumberLong\((-?\d+)\)", r"\1", clean_json)
            clean_json = re.sub("ISODate\((\S+)\)", r"\1", clean_json)
            try:
                return json.loads(clean_json)
            except JSONDecodeError as err:
                raise ValueError("Cannot parse JSON because of error {}:\n{}".format(err, repr(clean_json)))

        exception_search = re.search(r"exception: ([^\n]+)", exec_response)
        if exception_search:
            raise ValueError(exception_search.group(1).strip(": "))

        error_search = re.search(r"Error: (.+)", exec_response)
        if error_search:
            raise ValueError(error_search.group(1).strip(": "))

        # MongoDB often returns an empty status when it's starting up.
        logging.info("Cannot find any JSON or error in the MongoDB response: %s", repr(exec_response))
        return {}

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
