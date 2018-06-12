# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import subprocess

from typing import Iterator, Dict, Tuple

from croniter import croniter
from datetime import datetime

from mongoOperator.helpers.MongoResources import MongoResources
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration


class BackupChecker:
    """
    Class responsible for handling the Backups for the Mongo cluster.
    """

    def __init__(self, kubernetes_service: KubernetesService):
        self.kubernetes_service = kubernetes_service
        self.start_date = datetime.utcnow()
        self._cluster_cron_iterators = {}  # type: Dict[Tuple[str, str], Iterator[datetime]]

    def getNextTime(self, cluster_object: V1MongoClusterConfiguration) -> datetime:
        key = (cluster_object.metadata.name, cluster_object.metadata.namespace)
        iter = self._cluster_cron_iterators.get(key, None)
        if not iter:
            cron_config = cluster_object.spec.backups.cron
            iter = croniter(cron_config, self.start_date)
            self._cluster_cron_iterators[key] = iter
            return datetime.utcnow()
        return iter.get_next(datetime)

    def backupIfNeeded(self, cluster_object: V1MongoClusterConfiguration):
        next_backup = self.getNextTime(cluster_object)
        if next_backup <= datetime.utcnow():
            backup_file = self.backup(cluster_object)
            self.uploadBackup(cluster_object, backup_file)
        else:
            logging.info("Cluster %s @ ns/%s will need a backup at %s.", cluster_object.metadata.name,
                         cluster_object.metadata.namespace, next_backup.isoformat())

    def backup(self, cluster_object: V1MongoClusterConfiguration) -> str:

        backup_file = "/temp/mongodb-backup-{}-{}-{}.gz".format(cluster_object.metadata.namespace,
                                                               cluster_object.metadata.name,
                                                               datetime.utcnow().strftime('%Y_%m_%d_%H%M%S'))

        pod_index = cluster_object.spec.mongodb.replicas - 1  # last pod
        hostname = MongoResources.getMemberHostname(pod_index, cluster_object.metadata.name,
                                                    cluster_object.metadata.namespace)

        logging.info("Backing up cluster %s @ ns/%s from %s to %s.", cluster_object.metadata.name,
                     cluster_object.metadata.namespace, hostname, backup_file)

        backup_output = subprocess.check_output([
            "mongodump", "--host", hostname, "--out", backup_file
        ], stderr=STDOUT)

        logging.info("Backup output: %s", backup_output)
        return backup_file

    def uploadBackup(self, cluster_object: V1MongoClusterConfiguration, backup_file: str) -> None:
        pass # TODO
