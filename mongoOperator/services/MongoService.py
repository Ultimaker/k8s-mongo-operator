# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import re

from mongoOperator.helpers.MongoResources import MongoResources
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService


class MongoService:
    """
    Bundled methods for interacting with MongoDB.
    """
    CONTAINER = "mongodb"

    def __init__(self, kubernetes_service: KubernetesService):
        self.kubernetes_service = kubernetes_service

    def initializeReplicaSet(self, cluster_object: V1MongoClusterConfiguration) -> None:
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        replicas = cluster_object.spec.mongodb.replicas

        pod_name = "{}-0".format(name)
        replica_set_config = MongoResources.createReplicaSetConfig(cluster_object)
        command = "rs.initiate({})".format(json.dumps(replica_set_config))
        exec_command = MongoResources.createMongoExecCommand(command)

        exec_response = self.kubernetes_service.execInPod(self.CONTAINER, pod_name, namespace, exec_command)
        logging.info("Initializing replica, received %s", repr(exec_response))

        if '{ "ok" : 1 }' in exec_response:
            logging.info("initialized replica set {} in ns/{}".format(name, namespace))
        elif '"ok" : 0' in exec_response and '"codeName" : "NodeNotFound"' in exec_response:
            logging.info("waiting for {} {} replica set members in ns/{}".format(replicas, name, namespace))
            logging.debug(exec_response)
        else:
            logging.error("error initializing replica set {} in ns/{}\n{}".format(name, namespace, exec_response))

    def checkReplicaSetNeedsSetup(self, cluster_object: V1MongoClusterConfiguration) -> None:
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace

        pod_name = "{}-0".format(name)
        exec_command = MongoResources.createMongoExecCommand("rs.status()")
        exec_response = self.kubernetes_service.execInPod(self.CONTAINER, pod_name, namespace, exec_command)
        logging.debug("Checking replicas, received %s", repr(exec_response))

        # If the replica set is not initialized yet, we initialize it
        if '"ok" : 0' in exec_response :
            if '"codeName" : "NotYetInitialized"' in exec_response:
                self.initializeReplicaSet(cluster_object)
            else:
                logging.error("Replicas could not be checked in %s", repr(exec_response))

        # If we can get the replica set status without authenticating as the
        # admin user first, we have to create the users
        if '"ok" : 1' in exec_response:
            self.createUsers(cluster_object)

    def createUsers(self, cluster_object: V1MongoClusterConfiguration) -> bool:
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        replicas = cluster_object.spec.mongodb.replicas

        admin_credentials = self.kubernetes_service.getOperatorAdminSecret(name, namespace)
        command = MongoResources.createCreateUsersCommand(admin_credentials)

        for i in range(replicas):
            pod_name = "{}-{}".format(name, i)
            exec_command = MongoResources.createMongoExecCommand(command)
            exec_response = self.kubernetes_service.execInPod(self.CONTAINER, pod_name, namespace, exec_command)
            logging.debug("Received for pod %s: %s", i, repr(exec_response))

            if "Successfully added user: {" in exec_response:
                logging.info("Created users for %s in ns/%s", name, namespace)
                return True
            elif "Error: couldn't add user: not master :" in exec_response:
                # most of the time member 0 is elected master, otherwise we get this error and need to loop through
                # members until we find the master
                continue
            elif re.match(r"Error: couldn't add user: User .* already exists :", exec_response):
                continue
            else:
                logging.error("Error creating users for %s in ns/%s\n%s", name, namespace, repr(exec_response))
                return False
