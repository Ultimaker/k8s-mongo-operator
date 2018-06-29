# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from typing import Dict

from time import sleep

from kubernetes.client.rest import ApiException

from mongoOperator.helpers.AdminSecretChecker import AdminSecretChecker
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

    def _execInPod(self, pod_index: int, name: str, namespace: str, mongo_command: str) -> Dict[str, any]:
        """
        Executes the given mongo command inside the pod with the given name.
        Retries a few times in case we receive a handshake failure.
        :param pod_index: The index of the pod.
        :param name: The name of the cluster.
        :param namespace: The namespace of the cluster.
        :param mongo_command: The command to be executed in mongo.
        :return: The response from MongoDB. See files in `tests/fixtures/mongo_responses` for examples.
        :raise ValueError: If the result could not be parsed.
        :raise TimeoutError: If we could not connect to the pod after retrying.
        """
        exec_command = MongoResources.createMongoExecCommand(mongo_command)
        pod_name = "{}-{}".format(name, pod_index)

        for _ in range(self.EXEC_IN_POD_RETRIES):
            try:
                exec_response = self.kubernetes_service.execInPod(self.CONTAINER, pod_name, namespace, exec_command)
                response = MongoResources.parseMongoResponse(exec_response)
                if response and response.get("codeName") != "NodeNotFound":
                    return response
                logging.info("Waiting for replica set members for %s @ ns/%s: %s", pod_name, namespace, response)

            except ValueError as e:
                if str(e) not in ("connection attempt failed", "connect failed"):
                    raise
                logging.info("Could not connect to Mongo in pod %s @ ns/%s: %s", pod_name, namespace, e)

            except ApiException as e:
                if "Handshake status" not in e.reason:
                    logging.error("Error sending following command to pod %s: %s", pod_name, repr(mongo_command))
                    raise
                logging.info("Could not check the replica set or initialize it because of %s. The service is probably "
                             "starting up. We wait %s seconds before retrying.", e.reason, self.EXEC_IN_POD_WAIT)
            sleep(self.EXEC_IN_POD_WAIT)

        raise TimeoutError("Could not check the replica set after {} retries!".format(self.EXEC_IN_POD_RETRIES))

    def initializeReplicaSet(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """
        Initializes the replica set by sending an `initiate` command to the 1st Mongo pod.
        :param cluster_object: The cluster object from the YAML file.
        :raise ValueError: In case we receive an unexpected response from Mongo.
        :raise ApiException: In case we receive an unexpected response from Kubernetes.
        """
        cluster_name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace

        create_replica_command = MongoResources.createReplicaInitiateCommand(cluster_object)

        create_replica_response = self._execInPod(0, cluster_name, namespace, create_replica_command)

        logging.debug("Initializing replica, received %s", repr(create_replica_response))

        if create_replica_response["ok"] == 1:
            logging.info("Initialized replica set %s @ ns/%s", cluster_name, namespace)
        else:
            raise ValueError("Unexpected response initializing replica set {} @ ns/{}:\n{}"
                             .format(cluster_name, namespace, create_replica_response))

    def reconfigureReplicaSet(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """
        Initializes the replica set by sending a `reconfig` command to the 1st Mongo pod.
        :param cluster_object: The cluster object from the YAML file.
        :raise ValueError: In case we receive an unexpected response from Mongo.
        :raise ApiException: In case we receive an unexpected response from Kubernetes.
        """
        cluster_name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        replicas = cluster_object.spec.mongodb.replicas

        reconfigure_command = MongoResources.createReplicaReconfigureCommand(cluster_object)

        reconfigure_response = self._execInPod(0, cluster_name, namespace, reconfigure_command)

        logging.debug("Reconfiguring replica, received %s", repr(reconfigure_response))

        if reconfigure_response["ok"] == 1:
            logging.info("Reconfigured replica set %s @ ns/%s to %s pods", cluster_name, namespace, replicas)
        else:
            raise ValueError("Unexpected response reconfiguring replica set {} @ ns/{}:\n{}"
                             .format(cluster_name, namespace, reconfigure_response))

    def checkReplicaSetOrInitialize(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """
        Checks that the replica set is initialized, or initializes it otherwise.
        :param cluster_object: The cluster object from the YAML file.
        :raise ValueError: In case we receive an unexpected response from Mongo.
        :raise ApiException: In case we receive an unexpected response from Kubernetes.
        """
        cluster_name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        replicas = cluster_object.spec.mongodb.replicas

        create_status_command = MongoResources.createStatusCommand()

        create_status_response = self._execInPod(0, cluster_name, namespace, create_status_command)
        logging.debug("Checking replicas, received %s", repr(create_status_response))

        # If the replica set is not initialized yet, we initialize it
        if create_status_response["ok"] == 0 and create_status_response["codeName"] == "NotYetInitialized":
            return self.initializeReplicaSet(cluster_object)

        elif create_status_response["ok"] == 1:
            logging.info("The replica set %s @ ns/%s seems to be working properly with %s/%s pods.",
                         cluster_name, namespace, len(create_status_response["members"]), replicas)
            if replicas != len(create_status_response["members"]):
                self.reconfigureReplicaSet(cluster_object)
        else:
            raise ValueError("Unexpected response trying to check replicas: '{}'".format(repr(create_status_response)))

    def createUsers(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """
        Creates the users required for each of the pods in the replica.
        :param cluster_object: The cluster object from the YAML file.
        :raise ValueError: In case we receive an unexpected response from Mongo.
        :raise ApiException: In case we receive an unexpected response from Kubernetes.
        """
        cluster_name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        replicas = cluster_object.spec.mongodb.replicas

        secret_name = AdminSecretChecker.getSecretName(cluster_name)
        admin_credentials = self.kubernetes_service.getSecret(secret_name, namespace)
        create_admin_command = MongoResources.createCreateAdminCommand(admin_credentials)

        logging.info("Creating users for %s pods", replicas)

        for _ in range(self.EXEC_IN_POD_RETRIES):
            for i in range(replicas):
                # see tests for examples of these responses.
                try:
                    exec_response = self._execInPod(i, cluster_name, namespace, create_admin_command)
                    if "user" in exec_response:
                        logging.info("Created users for pod %s-%s @ ns/%s", cluster_name, i, namespace)
                        return

                    raise ValueError("Unexpected response creating users for pod {}-{} @ ns/{}:\n{}"
                                     .format(cluster_name, i, namespace, exec_response))

                except ValueError as err:
                    err_str = str(err)

                    if "couldn't add user: not master" in err_str:
                        # most of the time member 0 is elected master, otherwise we get this error and need to loop through
                        # members until we find the master
                        logging.info("The user could not be created in pod %s-%s because it's not master.", cluster_name, i)
                        continue

                    if "already exists" in err_str:
                        logging.info("User creation not necessary: %s", err_str)
                        return

                    raise

            logging.info("Could not create users in any of the %s pods of cluster %s @ ns/%s. We wait %s seconds "
                         "before retrying.", replicas, cluster_name, namespace, self.EXEC_IN_POD_WAIT)
            sleep(self.EXEC_IN_POD_WAIT)

        raise TimeoutError("Could not create users in any of the {} pods of cluster {} @ ns/{}."
                           .format(replicas, cluster_name, namespace))
