# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from kubernetes.watch import Watch
from typing import Dict, List, Tuple, Optional

from mongoOperator.helpers.AdminSecretChecker import AdminSecretChecker
from mongoOperator.helpers.BaseResourceChecker import BaseResourceChecker
from mongoOperator.helpers.ServiceChecker import ServiceChecker
from mongoOperator.helpers.StatefulSetChecker import StatefulSetChecker
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService
from mongoOperator.services.MongoService import MongoService


class ClusterChecker:
    """
    Manager that periodically checks the status of the MongoDB objects in the cluster.
    """

    STREAM_REQUEST_TIMEOUT = 5.0

    def __init__(self):
        self.kubernetes_service = KubernetesService()
        self.mongo_service = MongoService(self.kubernetes_service)

        self.resource_checkers = [
            ServiceChecker(self.kubernetes_service),
            StatefulSetChecker(self.kubernetes_service),
            AdminSecretChecker(self.kubernetes_service),
        ]  # type: List[BaseResourceChecker]

        self.cluster_versions = {}  # type: Dict[Tuple[str, str], str]  # format: {(cluster_name, namespace): resource_version}

    @staticmethod
    def _parseConfiguration(cluster_dict: Dict[str, any]) -> Optional[V1MongoClusterConfiguration]:
        """
        Tries to parse the given cluster configuration, returning None if the object cannot be parsed.
        :param cluster_dict: The dictionary containing the configuration.
        :return: The cluster configuration model, if valid, or None.
        """
        try:
            return V1MongoClusterConfiguration(**cluster_dict)
        except (ValueError, AttributeError) as err:
            meta = cluster_dict.get("metadata", {})
            logging.error("Could not validate cluster configuration for {} @ ns/{}: {}. The cluster will be ignored."
                          .format(meta.get("name"), meta.get("namespace"), err))

    def checkExistingClusters(self) -> None:
        """
        Check all Mongo objects and see if the sub objects are available.
        If they are not, they should be (re-)created to ensure the cluster is in the expected state.
        """
        mongo_objects = self.kubernetes_service.listMongoObjects()
        logging.info("Checking %s mongo objects.", len(mongo_objects["items"]))
        for cluster_dict in mongo_objects["items"]:
            cluster_object = self._parseConfiguration(cluster_dict)
            if cluster_object:
                self.checkCluster(cluster_object)

    def streamEvents(self) -> None:
        """
        Watches for changes to the mongo objects in Kubernetes and processes any changes immediately.
        """
        event_watcher = Watch()

        # start watching from the latest version that we have
        if self.cluster_versions:
            event_watcher.resource_version = max(self.cluster_versions.values())

        for event in event_watcher.stream(self.kubernetes_service.listMongoObjects, _request_timeout = self.STREAM_REQUEST_TIMEOUT):
            logging.info("Received event %s", event)

            if event["type"] in ("ADDED", "MODIFIED"):
                cluster_object = self._parseConfiguration(event["object"])
                if cluster_object:
                    self.checkCluster(cluster_object)
                    # we change the resource version manually because of a bug fixed only in a later version of K8s:
                    # https://github.com/kubernetes-client/python-base/pull/64
                    event_watcher.resource_version = cluster_object.metadata.resource_version
                else:
                    logging.warning("Could not validate cluster object, stopping event watcher.")
                    event_watcher.stop = True
            elif event["type"] in ("DELETED",):
                self.collectGarbage()

            else:
                logging.warning("Could not parse event, stopping event watcher.")
                event_watcher.stop = True

    def collectGarbage(self) -> None:
        """
        Cleans up any resources that are left after a cluster has been removed.
        """
        for checker in self.resource_checkers:
            checker.cleanResources()

    def checkCluster(self, cluster_object: V1MongoClusterConfiguration, force: bool = False) -> None:
        """
        Checks whether the given cluster is configured and updated.
        :param cluster_object: The cluster object from the YAML file.
        :param force: If this is True, we will re-update the cluster even if it has been checked before.
        """
        key = (cluster_object.metadata.name, cluster_object.metadata.namespace)
        
        if self.cluster_versions.get(key) == cluster_object.metadata.resource_version and not force:
            logging.debug("Cluster object %s has been checked already in version %s.",
                          key, cluster_object.metadata.resource_version)
            # we still want to check the replicas to make sure everything is working.
            self.mongo_service.checkReplicaSetOrInitialize(cluster_object)
        else:
            for checker in self.resource_checkers:
                checker.checkResource(cluster_object)
            self.mongo_service.checkReplicaSetOrInitialize(cluster_object)
            self.mongo_service.createUsers(cluster_object)
            self.cluster_versions[key] = cluster_object.metadata.resource_version
