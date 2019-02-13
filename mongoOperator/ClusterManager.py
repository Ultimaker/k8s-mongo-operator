# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from typing import Dict, List, Tuple, Optional

from mongoOperator.helpers.resourceCheckers.AdminSecretChecker import AdminSecretChecker
from mongoOperator.helpers.BackupHelper import BackupHelper
from mongoOperator.helpers.resourceCheckers.BaseResourceChecker import BaseResourceChecker
from mongoOperator.helpers.resourceCheckers.ServiceChecker import ServiceChecker
from mongoOperator.helpers.resourceCheckers.StatefulSetChecker import StatefulSetChecker
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService
from mongoOperator.services.MongoService import MongoService


class ClusterChecker:
    """ Manager that periodically checks the status of the MongoDB objects in the cluster. """

    STREAM_REQUEST_TIMEOUT = (15.0, 5.0)  # connect, read timeout

    def __init__(self) -> None:
        self._cluster_versions: Dict[Tuple[str, str], str] = {}  # format: {(cluster_name, namespace): resource_version}
        self._kubernetes_service = KubernetesService()
        self._mongo_service = MongoService(self._kubernetes_service)
        self._backup_checker = BackupHelper(self._kubernetes_service)
        self._resource_checkers: List[BaseResourceChecker] = [
            ServiceChecker(self._kubernetes_service),
            StatefulSetChecker(self._kubernetes_service),
            AdminSecretChecker(self._kubernetes_service),
        ]

    def checkExistingClusters(self) -> None:
        """
        Check all Mongo objects and see if the sub objects are available.
        If they are not, they should be (re-)created to ensure the cluster is in the expected state.
        """
        mongo_objects = self._kubernetes_service.listMongoObjects()
        logging.info("Checking %s mongo objects.", len(mongo_objects["items"]))
        for cluster_dict in mongo_objects["items"]:
            cluster_object = self._parseConfiguration(cluster_dict)
            if cluster_object:
                self._checkCluster(cluster_object)

    def collectGarbage(self) -> None:
        """
        Cleans up any resources that are left after a cluster has been removed.
        """
        for checker in self._resource_checkers:
            checker.cleanResources()

    def _checkCluster(self, cluster_object: V1MongoClusterConfiguration, force: bool = False) -> None:
        """
        Checks whether the given cluster is configured and updated.
        :param cluster_object: The cluster object from the YAML file.
        :param force: If this is True, we will re-update the cluster even if it has been checked before.
        """
        key = cluster_object.metadata.name, cluster_object.metadata.namespace
        
        if self._cluster_versions.get(key) == cluster_object.metadata.resource_version and not force:
            logging.debug("Cluster object %s has been checked already in version %s.",
                          key, cluster_object.metadata.resource_version)
            # we still want to check the replicas to make sure everything is working.
            self._mongo_service.checkOrCreateReplicaSet(cluster_object)
        else:
            for checker in self._resource_checkers:
                checker.checkResource(cluster_object)
            self._mongo_service.checkOrCreateReplicaSet(cluster_object)
            self._mongo_service.createUsers(cluster_object)
            self._cluster_versions[key] = cluster_object.metadata.resource_version

        self._backup_checker.backupIfNeeded(cluster_object)

    @staticmethod
    def _parseConfiguration(cluster_dict: Dict[str, any]) -> Optional[V1MongoClusterConfiguration]:
        """
        Tries to parse the given cluster configuration, returning None if the object cannot be parsed.
        :param cluster_dict: The dictionary containing the configuration.
        :return: The cluster configuration model, if valid, or None.
        """
        try:
            result = V1MongoClusterConfiguration(**cluster_dict)
            result.validate()
            return result
        except ValueError as err:
            meta = cluster_dict.get("metadata", {})
            logging.error("Could not validate cluster configuration for {} @ ns/{}: {}. The cluster will be ignored."
                          .format(meta.get("name"), meta.get("namespace"), err))
