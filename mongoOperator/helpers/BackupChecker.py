# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import os
from base64 import b64decode
from subprocess import check_output, STDOUT, CalledProcessError, SubprocessError

from croniter import croniter
from datetime import datetime
from google.cloud.storage import Client as StorageClient
from google.oauth2.service_account import Credentials as ServiceCredentials
from typing import Dict, Tuple

from mongoOperator.helpers.MongoResources import MongoResources
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService


class BackupChecker:
    """
    Class responsible for handling the Backups for the Mongo cluster.
    """
    DEFAULT_BACKUP_PREFIX = "backups"
    BACKUP_FILE_FORMAT = "mongodb-backup-{namespace}-{name}-{date}.archive.gz"

    def __init__(self, kubernetes_service: KubernetesService):
        self.kubernetes_service = kubernetes_service
        self.start_date = datetime.utcnow()
        self._cluster_cron_iterators = {}  # type: Dict[Tuple[str, str], croniter]

    def getNextTime(self, cluster_object: V1MongoClusterConfiguration) -> datetime:
        key = (cluster_object.metadata.name, cluster_object.metadata.namespace)
        cron_iter = self._cluster_cron_iterators.get(key, None)
        if not cron_iter:
            cron_config = cluster_object.spec.backups.cron
            cron_iter = croniter(cron_config, self.start_date, datetime)
            self._cluster_cron_iterators[key] = cron_iter
            return datetime.utcnow()
        return cron_iter.get_next()

    def backupIfNeeded(self, cluster_object: V1MongoClusterConfiguration):
        next_backup = self.getNextTime(cluster_object)
        if next_backup <= datetime.utcnow():
            backup_file = self.backup(cluster_object)
            self.uploadBackup(cluster_object, backup_file)
        else:
            logging.info("Cluster %s @ ns/%s will need a backup at %s.", cluster_object.metadata.name,
                         cluster_object.metadata.namespace, next_backup.isoformat())

    def backup(self, cluster_object: V1MongoClusterConfiguration) -> str:

        backup_file = "/tmp/" + self.BACKUP_FILE_FORMAT.format(namespace=cluster_object.metadata.namespace,
                                                               name=cluster_object.metadata.name,
                                                               date=datetime.utcnow().strftime('%Y-%m-%d_%H%M%S'))

        pod_index = cluster_object.spec.mongodb.replicas - 1  # last pod
        hostname = MongoResources.getMemberHostname(pod_index, cluster_object.metadata.name,
                                                    cluster_object.metadata.namespace)

        logging.info("Backing up cluster %s @ ns/%s from %s to %s.", cluster_object.metadata.name,
                     cluster_object.metadata.namespace, hostname, backup_file)

        try:
            backup_output = check_output(
                ["mongodump", "--host", hostname, "--gzip", "--archive=" + backup_file],
                stderr=STDOUT
            )
        except CalledProcessError as err:
            raise SubprocessError("Could not backup {} to {}. Return code: {}\n stderr: {}\n stdout: {}"
                                  .format(hostname, backup_file, err.returncode, err.stderr, err.stdout))

        logging.debug("Backup output: %s", backup_output)
        return backup_file

    def uploadBackup(self, cluster_object: V1MongoClusterConfiguration, backup_file: str) -> None:
        secret_key = cluster_object.spec.backups.gcs.service_account.secret_key_ref

        secret = self.kubernetes_service.getSecret(secret_key["name"], cluster_object.metadata.namespace)
        credentials_json = b64decode(secret.data[secret_key["key"]])

        prefix = cluster_object.spec.backups.gcs.prefix or self.DEFAULT_BACKUP_PREFIX

        self._uploadFile(
            credentials=json.loads(credentials_json),
            bucket_name=cluster_object.spec.backups.gcs.bucket,
            key="{}/{}".format(prefix, backup_file.replace("/tmp/", "")),
            file_name=backup_file
        )

        os.remove(backup_file)

    @staticmethod
    def _uploadFile(credentials: dict, bucket_name: str, key: str, file_name: str):
        credentials = ServiceCredentials.from_service_account_info(credentials)
        gcs_client = StorageClient(credentials.project_id, credentials)
        bucket = gcs_client.bucket(bucket_name)
        bucket.blob(key).upload_from_filename(file_name)
        logging.info("Backup uploaded to gcs://%s/%s", bucket_name, key)
