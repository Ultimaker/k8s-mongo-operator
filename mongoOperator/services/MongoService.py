# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
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
        """
        Initializes the replica set by sending an `initiate` command to the 1st Mongo pod.
        :param cluster_object: The cluster object from the YAML file.
        :raise ValueError: In case we receive an unexpected response from Mongo.
        :raise ApiException: In case we receive an unexpected response from Kubernetes.
        """
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        replicas = cluster_object.spec.mongodb.replicas

        pod_name = "{}-0".format(name)
        mongo_command = MongoResources.createReplicaInitiateCommand(cluster_object)
        exec_command = MongoResources.createMongoExecCommand(mongo_command)

        # see tests for examples of these responses.
        exec_response = self.kubernetes_service.execInPod(self.CONTAINER, pod_name, namespace, exec_command)
        logging.debug("Initializing replica, received %s", repr(exec_response))

        if '"ok" : 1' in exec_response:
            logging.info("USE FOR TESTS initializeReplicaSet: %s", exec_response)
            logging.info("initialized replica set {} in ns/{}".format(name, namespace))
        elif '"ok" : 0' in exec_response and '"codeName" : "NodeNotFound"' in exec_response:
            logging.info("USE FOR TESTS initializeReplicaSet: %s", exec_response)
            logging.info("waiting for {} {} replica set members in ns/{}".format(replicas, name, namespace))
        else:
            raise ValueError("Unexpected response initializing replica set {} in ns/{}:\n{}"
                             .format(name, namespace, exec_response))

    def checkReplicaSetOrInitialize(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """
        Checks that the replica set is initialized, or initializes it otherwise.
        :param cluster_object: The cluster object from the YAML file.
        :raise ValueError: In case we receive an unexpected response from Mongo.
        :raise ApiException: In case we receive an unexpected response from Kubernetes.
        """
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace

        mongo_command = MongoResources.createStatusCommand()
        exec_command = MongoResources.createMongoExecCommand(mongo_command)

        # see tests for examples of these responses.
        pod_name = "{}-0".format(name)
        exec_response = self.kubernetes_service.execInPod(self.CONTAINER, pod_name, namespace, exec_command)
        logging.debug("Checking replicas, received %s", repr(exec_response))

        # If the replica set is not initialized yet, we initialize it
        if '"ok" : 0,' in exec_response and '"codeName" : "NotYetInitialized"' in exec_response:
            logging.info("USE FOR TESTS checkReplicaSetOrInitialize OK=0: %s", exec_response)
            return self.initializeReplicaSet(cluster_object)

        # If we can get the replica set status without authenticating as the admin user first, we create the users
        elif '"ok" : 1,' in exec_response:
            return self.createUsers(cluster_object)

        elif "connection attempt failed" in exec_response:
            logging.info("Could not connect to the pod {} @ ns/{}: {}".format(pod_name, namespace, repr(exec_response)))
            return

        raise ValueError("Unexpected response trying to check replicas: %s", repr(exec_response))

    def createUsers(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """
        Creates the users required for each of the pods in the replica.
        :param cluster_object: The cluster object from the YAML file.
        :raise ValueError: In case we receive an unexpected response from Mongo.
        :raise ApiException: In case we receive an unexpected response from Kubernetes.
        """
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        replicas = cluster_object.spec.mongodb.replicas

        admin_credentials = self.kubernetes_service.getOperatorAdminSecret(name, namespace)
        mongo_command = MongoResources.createCreateAdminCommand(admin_credentials)

        for i in range(replicas):
            pod_name = "{}-{}".format(name, i)
            exec_command = MongoResources.createMongoExecCommand(mongo_command)

            # see tests for examples of these responses.
            exec_response = self.kubernetes_service.execInPod(self.CONTAINER, pod_name, namespace, exec_command)
            logging.debug("Received for pod %s: %s", i, repr(exec_response))

            if "Error: couldn't add user: not master :" in exec_response:
                # most of the time member 0 is elected master, otherwise we get this error and need to loop through
                # members until we find the master
                logging.info("USE FOR TESTS createUsers (not master): %s", exec_response)
                continue

            if "Successfully added user: {" in exec_response:
                logging.info("Created users for %s in ns/%s", name, namespace)
                logging.info("USE FOR TESTS createUsers: %s", exec_response)
                return

            if re.search(r"Error: couldn\\*'t add user: User \S+ already exists :", exec_response):
                logging.debug("The user already exists, skipping.")
                return

            raise ValueError("Unexpected response creating users for %s in ns/%s\n%s", name, namespace, repr(exec_response))
