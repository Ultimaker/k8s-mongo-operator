# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from time import sleep
from typing import Dict, Optional, List

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

from mongoOperator.helpers.AdminSecretChecker import AdminSecretChecker
from mongoOperator.helpers.MongoResources import MongoResources
from mongoOperator.helpers.RestoreHelper import RestoreHelper
from mongoOperator.helpers.listeners.mongo.CommandLogger import CommandLogger
from mongoOperator.helpers.listeners.mongo.HeartbeatListener import HeartbeatListener
from mongoOperator.helpers.listeners.mongo.ServerLogger import ServerLogger
from mongoOperator.helpers.listeners.mongo.TopologyListener import TopologyListener
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService


class MongoService:
    """ Bundled methods for interacting with MongoDB. """
    
    # name of the container
    CONTAINER = "mongodb"
    NO_REPLICA_SET_RESPONSE = "no replset config has been received"

    # after creating a new object definition we can get handshake failures.
    # below we can configure how many times we retry and how long we wait in between.
    MONGO_COMMAND_RETRIES = 4
    MONGO_COMMAND_WAIT = 15.0

    def __init__(self, kubernetes_service: KubernetesService) -> None:
        self._kubernetes_service = kubernetes_service
        self._restore_helper = RestoreHelper(self._kubernetes_service)
        self._connected_replica_sets: Dict[str, MongoClient] = {}
        self._restored_cluster_names: List[str] = []

    def checkOrCreateReplicaSet(self, cluster_object: V1MongoClusterConfiguration) -> None:
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
            create_status_response = self._executeAdminCommand(cluster_object, create_status_command)
            logging.debug("Checking replicas, received %s", repr(create_status_response))
            
            # The replica set could not be checked
            if create_status_response["ok"] != 1:
                raise ValueError("Unexpected response trying to check replicas: '{}'".format(
                        repr(create_status_response)))

            logging.info("The replica set %s @ ns/%s seems to be working properly with %s/%s pods.",
                         cluster_name, namespace, len(create_status_response["members"]), replicas)

            # The amount of replicas is not the same as configured, we need to fix this
            if replicas != len(create_status_response["members"]):
                self._reconfigureReplicaSet(cluster_object)

        except OperationFailure as err:
            if str(err) != self.NO_REPLICA_SET_RESPONSE:
                raise

            # If the replica set is not initialized yet, we initialize it
            self._initializeReplicaSet(cluster_object)

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
        admin_credentials = self._kubernetes_service.getSecret(secret_name, namespace)
        create_admin_command, create_admin_args, create_admin_kwargs = MongoResources.createCreateAdminCommand(
            admin_credentials)
        create_admin_response = self._executeAdminCommand(cluster_object, create_admin_command, create_admin_args,
                                                          **create_admin_kwargs)
        logging.info("Created admin user: %s", create_admin_response)

    def _reconfigureReplicaSet(self, cluster_object: V1MongoClusterConfiguration) -> None:
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
        reconfigure_response = self._executeAdminCommand(cluster_object, reconfigure_command, reconfigure_args)

        logging.debug("Reconfiguring replica, received %s", repr(reconfigure_response))

        if reconfigure_response["ok"] != 1:
            raise ValueError("Unexpected response reconfiguring replica set {} @ ns/{}:\n{}"
                             .format(cluster_name, namespace, reconfigure_response))

        logging.info("Reconfigured replica set %s @ ns/%s to %s pods", cluster_name, namespace, replicas)

    @staticmethod
    def _initializeReplicaSet(cluster_object: V1MongoClusterConfiguration) -> None:
        """
        Initializes the replica set by sending an `initiate` command to the 1st Mongo pod.
        :param cluster_object: The cluster object from the YAML file.
        :raise ValueError: In case we receive an unexpected response from Mongo.
        :raise ApiException: In case we receive an unexpected response from Kubernetes.
        """
        cluster_name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
    
        master_connection = MongoClient(MongoResources.getMemberHostname(0, cluster_name, namespace))
        create_replica_command, create_replica_args = MongoResources.createReplicaInitiateCommand(cluster_object)
        create_replica_response = master_connection.admin.command(create_replica_command, create_replica_args)
    
        if create_replica_response["ok"] == 1:
            logging.info("Initialized replica set %s @ ns/%s", cluster_name, namespace)
            return
    
        logging.error("Initializing replica set failed, received %s", repr(create_replica_response))
        raise ValueError("Unexpected response initializing replica set {} @ ns/{}:\n{}"
                         .format(cluster_name, namespace, create_replica_response))
        
    def _createMongoClientForReplicaSet(self, replica_set_name: str, cluster_object: V1MongoClusterConfiguration
                                        ) -> MongoClient:
        """
        Creates a new MongoClient instance for a replica set.
        :param replica_set_name: The name of the replica set.
        :return: The mongo client.
        """
        return MongoClient(
            MongoResources.getMemberHostnames(cluster_object),
            connectTimeoutMS = 60000,
            serverSelectionTimeoutMS = 60000,
            replicaSet = replica_set_name,
            event_listeners = [
                CommandLogger(),
                ServerLogger(),
                TopologyListener(cluster_object, replica_set_ready_callback = self._onReplicaSetReady),
                HeartbeatListener(cluster_object, all_hosts_ready_callback = self._onAllHostsReady)
            ]
        )

    def _onReplicaSetReady(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """
        Callback triggered when a replica set is ready to be operated on.
        If a restore is still needed for the given replica set, it will be executed at this stage.
        :param cluster_object: The cluster configuration object for the replica set.
        """
        if cluster_object.metadata.name in self._restored_cluster_names:
            # A restore was already done for this replica set, so we don't have to do anything.
            return
        self._restore_helper.restoreIfNeeded(cluster_object)
        self._restored_cluster_names.append(cluster_object.metadata.name)

    def _onAllHostsReady(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """
        Callback triggered when all hosts in the would-be replica set are available.
        :param cluster_object: The cluster configuration object for the hosts in the would-be replica set.
        """
        self._initializeReplicaSet(cluster_object)

    def _executeAdminCommand(self, cluster_object: V1MongoClusterConfiguration, mongo_command: str, *args, **kwargs
                             ) -> Optional[Dict[str, any]]:
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
                replica_set_name = cluster_object.metadata.name
                if replica_set_name not in self._connected_replica_sets:
                    self._connected_replica_sets[replica_set_name] = self._createMongoClientForReplicaSet(
                            replica_set_name, cluster_object)
                return self._connected_replica_sets[replica_set_name].admin.command(mongo_command, *args, **kwargs)
            except ConnectionFailure as err:
                logging.error("Exception while trying to connect to Mongo: %s", str(err))
            logging.info("Command timed out, waiting %s seconds before trying again (attempt %s/%s)",
                         self.MONGO_COMMAND_WAIT, _, self.MONGO_COMMAND_RETRIES)
            sleep(self.MONGO_COMMAND_WAIT)

        raise TimeoutError("Could not execute command after {} retries!".format(self.MONGO_COMMAND_RETRIES))
