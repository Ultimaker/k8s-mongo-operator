# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from time import sleep

import re
from kubernetes.client.rest import ApiException

from mongoOperator.helpers.MongoResources import MongoResources
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService


class MongoService:
    """
    Bundled methods for interacting with MongoDB.
    """
    CONTAINER = "mongodb"

    # after creating a new object definition we can get handshake failures.
    # below we can configure how many times we retry and how long we wait in between.
    EXEC_IN_POD_RETRIES = 4
    EXEC_IN_POD_WAIT = 15.0

    def __init__(self, kubernetes_service: KubernetesService):
        self.kubernetes_service = kubernetes_service

    def _execInPod(self, pod_index: int, name: str, namespace: str, mongo_command: str) -> str:
        """
        Executes the given mongo command inside the pod with the given name.
        Retries a few times in case we receive a handshake failure.
        :param pod_index: The index of the pod.
        :param name: The name of the cluster.
        :param namespace: The namespace of the cluster.
        :param mongo_command: The command to be executed in mongo.
        :return: The response from MongoDB. See files in `tests/fixtures/mongo_responses` for examples.
        """
        exec_command = MongoResources.createMongoExecCommand(mongo_command)
        pod_name = "{}-{}".format(name, pod_index)

        for _ in range(self.EXEC_IN_POD_RETRIES):
            try:
                exec_response = self.kubernetes_service.execInPod(self.CONTAINER, pod_name, namespace, exec_command)
                if "connection attempt failed" in exec_response:
                    logging.info("Could not connect to Mongo in pod %s @ ns/%s: %s", pod_name, namespace,
                                 repr(exec_response))
                elif '"ok" : 0' in exec_response and '"codeName" : "NodeNotFound"' in exec_response:
                    logging.info("Waiting for replica set members for %s @ ns/%s: %s", pod_name, namespace)
                else:
                    return exec_response

            except ApiException as e:
                if "Handshake status" not in e.reason:
                    logging.error("Error sending following command to pod %s: %s", pod_name, repr(mongo_command))
                    raise
                logging.info("Could not check the replica set or initialize it because of %s. The service is probably "
                             "starting up. We wait %s seconds before retrying.", e.reason, self.EXEC_IN_POD_WAIT)
            sleep(self.EXEC_IN_POD_WAIT)

        raise TimeoutError("Could not check the replica set after {} retries".format(self.EXEC_IN_POD_RETRIES))

    def initializeReplicaSet(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """
        Initializes the replica set by sending an `initiate` command to the 1st Mongo pod.
        :param cluster_object: The cluster object from the YAML file.
        :raise ValueError: In case we receive an unexpected response from Mongo.
        :raise ApiException: In case we receive an unexpected response from Kubernetes.
        """
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace

        mongo_command = MongoResources.createReplicaInitiateCommand(cluster_object)

        exec_response = self._execInPod(0, name, namespace, mongo_command)

        logging.debug("Initializing replica, received %s", repr(exec_response))

        if '"ok" : 1' in exec_response:
            logging.info("Initialized replica set %s @ ns/%s", name, namespace)
        else:
            raise ValueError("Unexpected response initializing replica set {} @ ns/{}:\n{}"
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

        exec_response = self._execInPod(0, name, namespace, mongo_command)
        logging.debug("Checking replicas, received %s", repr(exec_response))

        # If the replica set is not initialized yet, we initialize it
        if '"ok" : 0,' in exec_response and '"codeName" : "NotYetInitialized"' in exec_response:
            return self.initializeReplicaSet(cluster_object)

        # If we can get the replica set status without authenticating as the admin user first, we create the users
        elif '"ok" : 1,' in exec_response:
            # TODO: Parse this response, we may need to change the number of replicas.
            logging.info("The replica set %s @ ns/%s seems to be working properly.", name, namespace)
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

        logging.info("Creating users for %s pods", replicas)

        for _ in range(self.EXEC_IN_POD_RETRIES):
            for i in range(replicas):
                # see tests for examples of these responses.
                exec_response = self._execInPod(i, name, namespace, mongo_command)
                logging.debug("Received for pod %s: %s", i, repr(exec_response))

                if "Error: couldn't add user: not master :" in exec_response:
                    # most of the time member 0 is elected master, otherwise we get this error and need to loop through
                    # members until we find the master
                    logging.info("The user could not be created in pod %s-%s because it's not master.", name, i)
                    continue

                if "Successfully added user: {" in exec_response:
                    logging.info("Created users for pod %s-%s @ ns/%s", name, i, namespace)
                    return

                if re.search(r"Error: couldn\\*'t add user: User \S+ already exists :", exec_response):
                    logging.info("The user already exists, skipping pod %s-%s.", name, i)
                    return

                raise ValueError("Unexpected response creating users for pod {}-{} @ ns/{}\n{}"
                                 .format(name, i, namespace, repr(exec_response)))

            logging.info("Could not create users in any of the %s pods of cluster %s @ ns/%s. We wait %s seconds "
                         "before retrying.", replicas, name, namespace, self.EXEC_IN_POD_WAIT)
            sleep(self.EXEC_IN_POD_WAIT)

        raise TimeoutError("Could not create users in any of the {} pods of cluster {} @ ns/{}."
                           .format(replicas, name, namespace))
