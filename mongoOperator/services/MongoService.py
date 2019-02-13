# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from typing import Dict

from time import sleep

from mongoOperator.helpers.AdminSecretChecker import AdminSecretChecker
from mongoOperator.helpers.MongoResources import MongoResources
from mongoOperator.helpers.RestoreHelper import RestoreHelper
from mongoOperator.helpers.MongoMonitoring import CommandLogger, TopologyLogger, ServerLogger, HeartbeatLogger

from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure


class MongoService:
    """
    Bundled methods for interacting with MongoDB.
    """
    CONTAINER = "mongodb"

    # after creating a new object definition we can get handshake failures.
    # below we can configure how many times we retry and how long we wait in between.
    MONGO_COMMAND_RETRIES = 4
    MONGO_COMMAND_WAIT = 15.0

    def __init__(self, kubernetes_service: KubernetesService):
        self.kubernetes_service = kubernetes_service
        self.restore_helper = RestoreHelper(self.kubernetes_service)
        self.mongo_connections = {}
        self.restores_done = []

    def _onReplicaSetReady(self, cluster_object: V1MongoClusterConfiguration) -> None:
        if cluster_object.metadata.name not in self.restores_done:
            # If restore was specified, load restore file
            self.restore_helper.restoreIfNeeded(cluster_object)
            self.restores_done.append(cluster_object.metadata.name)

    def _onAllHostsReady(self, cluster_object: V1MongoClusterConfiguration) -> None:
        self.initializeReplicaSet(cluster_object)

    def _mongoAdminCommand(self, cluster_object: V1MongoClusterConfiguration, mongo_command: str, *args, **kwargs) -> Dict[str, any]:
        """
        Executes the given mongo command inside the pod with the given name.
        Retries a few times in case we receive a handshake failure.
        :param name: The name of the cluster.
        :param namespace: The namespace of the cluster.
        :param mongo_command: The command to be executed in mongo.
        :return: The response from MongoDB. See files in `tests/fixtures/mongo_responses` for examples.
        :raise ValueError: If the result could not be parsed.
        :raise TimeoutError: If we could not connect after retrying.
        """
        for _ in range(self.MONGO_COMMAND_RETRIES):
            try:
                replicaset = cluster_object.metadata.name

                if replicaset not in self.mongo_connections:
                    self.mongo_connections[replicaset] = MongoClient(
                        MongoResources.getConnectionSeeds(cluster_object),
                        connectTimeoutMS=60000,
                        serverSelectionTimeoutMS=60000,
                        replicaSet=replicaset,
                        event_listeners=[CommandLogger(),
                                         TopologyLogger(cluster_object,
                                                        replica_set_ready_callback=self._onReplicaSetReady),
                                         ServerLogger(),
                                         HeartbeatLogger(cluster_object,
                                                         all_hosts_ready_callback=self._onAllHostsReady)
                                        ]
                    )

                return self.mongo_connections[replicaset].admin.command(mongo_command, *args, **kwargs)
            except ConnectionFailure as e:
                logging.error("Exception while trying to connect to Mongo: %s", str(e))

            logging.info("Command timed out, waiting %s seconds before trying again (attempt %s/%s)",
                         self.MONGO_COMMAND_WAIT, _, self.MONGO_COMMAND_RETRIES)

            sleep(self.MONGO_COMMAND_WAIT)

        raise TimeoutError("Could not execute command after {} retries!".format(self.MONGO_COMMAND_RETRIES))

    def initializeReplicaSet(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """
        Initializes the replica set by sending an `initiate` command to the 1st Mongo pod.
        :param cluster_object: The cluster object from the YAML file.
        :raise ValueError: In case we receive an unexpected response from Mongo.
        :raise ApiException: In case we receive an unexpected response from Kubernetes.
        """
        cluster_name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace

        create_replica_command, create_replica_args = MongoResources.createReplicaInitiateCommand(cluster_object)
        conn = MongoClient(MongoResources.getMemberHostname(0, cluster_name, namespace))
        create_replica_response = conn.admin.command(create_replica_command, create_replica_args)

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

        reconfigure_command, reconfigure_args = MongoResources.createReplicaReconfigureCommand(cluster_object)
        reconfigure_response = self._mongoAdminCommand(cluster_object, reconfigure_command, reconfigure_args)

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

        try:
            create_status_response = self._mongoAdminCommand(cluster_object, create_status_command)
            logging.debug("Checking replicas, received %s", repr(create_status_response))

            if create_status_response["ok"] == 1:
                logging.info("The replica set %s @ ns/%s seems to be working properly with %s/%s pods.",
                             cluster_name, namespace, len(create_status_response["members"]), replicas)
                if replicas != len(create_status_response["members"]):
                    self.reconfigureReplicaSet(cluster_object)
            else:
                raise ValueError("Unexpected response trying to check replicas: '{}'".format(
                    repr(create_status_response)))

        except OperationFailure as e:
            # If the replica set is not initialized yet, we initialize it
            if str(e) == "no replset config has been received":
                return self.initializeReplicaSet(cluster_object)
            raise

    def createUsers(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """
        Creates the users required for each of the pods in the replica.
        :param cluster_object: The cluster object from the YAML file.
        :raise ValueError: In case we receive an unexpected response from Mongo.
        :raise ApiException: In case we receive an unexpected response from Kubernetes.
        """
        cluster_name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace

        secret_name = AdminSecretChecker.getSecretName(cluster_name)
        admin_credentials = self.kubernetes_service.getSecret(secret_name, namespace)
        create_admin_command, create_admin_args, create_admin_kwargs = MongoResources.createCreateAdminCommand(
            admin_credentials)
        logging.info("Creating admin user.")
        create_admin_response = self._mongoAdminCommand(cluster_object, create_admin_command, create_admin_args,
                                                        **create_admin_kwargs)
        logging.info("Got response: %s", create_admin_response)
