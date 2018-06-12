# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import subprocess

from typing import Iterator, Dict, Tuple
from google.cloud.storage import Client as StorageClient, Bucket, Blob
from google.oauth2.service_account import Credentials as ServiceCredentials

from croniter import croniter
from datetime import datetime

from mongoOperator.helpers.MongoResources import MongoResources
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration


class BackupChecker:
    """
    Class responsible for handling the Backups for the Mongo cluster.
    """
    DEFAULT_BACKUP_PREFIX = "backups"
    BACKUP_FILE_FORMAT = "mongodb-backup-{namespace}-{name}-{date}.gz"

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

        backup_file = "/temp/" + self.BACKUP_FILE_FORMAT.format(namespace=cluster_object.metadata.namespace,
                                                                name=cluster_object.metadata.name,
                                                                date=datetime.utcnow().strftime('%Y_%m_%d_%H%M%S'))

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
        secret_key = cluster_object.spec.backups.gcs.service_account.secret_key_ref

        secret = self.kubernetes_service.getSecret(secret_key.name, cluster_object.metadata.namespace)

        credentials = ServiceCredentials.from_service_account_info(secret.data[secret_key.key])

        client = StorageClient(credentials.project_id, credentials)

        bucket = client.bucket(cluster_object.spec.backups.gcs.backup_name)

        path = cluster_object.spec.backups.gcs.prefix or self.DEFAULT_BACKUP_PREFIX

        key = "{}/{}".format(path, backup_file.replace("/temp/", ""))
        logging.info("Uploading backup to gcs://%s/%s", bucket.name, key)

        bucket.blob(key).upload_from_filename(backup_file)

        os.remove(backup_file)
