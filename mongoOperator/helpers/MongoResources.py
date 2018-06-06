# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from base64 import b64decode

from kubernetes import client

from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration


class MongoResources:
    """
    Helper class responsible for creating the Mongo commands.
    """

    DNS_SUFFIX = "svc.cluster.local"

    @classmethod
    def getMemberHostname(cls, member_id, cluster_name, namespace) -> str:
        return "{}-{}.{}.{}.{}".format(cluster_name, member_id, cluster_name, namespace, cls.DNS_SUFFIX)

    @classmethod
    def createReplicaSetConfig(cls, cluster_object: V1MongoClusterConfiguration):
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        replicas = cluster_object.spec.mongodb.replicas
        return {
            "_id": name,
            "version": 1,
            "members": [{"_id": i, "host": cls.getMemberHostname(i, name, namespace)} for i in range(replicas)]
        }

    @classmethod
    def createMongoExecCommand(cls, command: str):
        return [
            "mongo", "localhost:27017/admin",
            #TODO:
            # "--ssl",
            # "--sslCAFile", "/etc/ssl/mongod/ca.pem",
            # "--sslPEMKeyFile", "/etc/ssl/mongod/mongod.pem",
            "--eval", command
        ]

    @classmethod
    def createCreateUsersCommand(cls, admin_credentials: client.V1Secret) -> str:
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
