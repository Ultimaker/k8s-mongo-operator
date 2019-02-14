# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import os
from base64 import b64decode
from subprocess import check_output, CalledProcessError, SubprocessError

from croniter import croniter
from datetime import datetime
from google.cloud.storage import Client as StorageClient
from google.oauth2.service_account import Credentials as ServiceCredentials
from typing import Dict, Tuple

from mongoOperator.helpers.MongoResources import MongoResources
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService


class BackupHelper:
    """
    Class responsible for handling the Backups for the Mongo cluster.
    """
    DEFAULT_BACKUP_PREFIX = "backups"
    BACKUP_FILE_FORMAT = "mongodb-backup-{namespace}-{name}-{date}.archive.gz"

    @staticmethod
    def _utcNow() -> datetime:
        """
        :return: The current date in UTC timezone.
        """
        return datetime.utcnow()

    def __init__(self, kubernetes_service: KubernetesService):
        """
        :param kubernetes_service: The kubernetes service.
        """
        self.kubernetes_service = kubernetes_service
        self._last_backups = {}  # type: Dict[Tuple[str, str], datetime]  # format: {(cluster_name, namespace): date}

    def backupIfNeeded(self, cluster_object: V1MongoClusterConfiguration) -> bool:
        """
        Checks whether a backup is needed for the cluster, backing it up if necessary.
        :param cluster_object: The cluster object from the YAML file.
        :return: Whether a backup was created or not.
        """
        now = self._utcNow()

        cluster_key = (cluster_object.metadata.name, cluster_object.metadata.namespace)
        last_backup = self._last_backups.get(cluster_key)
        next_backup = croniter(cluster_object.spec.backups.cron, last_backup, datetime).get_next() \
            if last_backup else now

        if next_backup <= now:
            self.backup(cluster_object, now)
            self._last_backups[cluster_key] = now
            return True

        logging.info("Cluster %s @ ns/%s will need a backup at %s.", cluster_object.metadata.name,
                     cluster_object.metadata.namespace, next_backup.isoformat())
        return False

    def backup(self, cluster_object: V1MongoClusterConfiguration, now: datetime):
        """
        Creates a new backup for the given cluster saving it in the cloud storage.
        :param cluster_object: The cluster object from the YAML file.
        :param now: The current date, used in the date format.
        """
        backup_file = "/tmp/" + self.BACKUP_FILE_FORMAT.format(namespace=cluster_object.metadata.namespace,
                                                               name=cluster_object.metadata.name,
                                                               date=now.strftime("%Y-%m-%d_%H%M%S"))

        pod_index = cluster_object.spec.mongodb.replicas - 1  # take last pod
        hostname = MongoResources.getMemberHostname(pod_index, cluster_object.metadata.name,
                                                    cluster_object.metadata.namespace)

        logging.info("Backing up cluster %s @ ns/%s from %s to %s.", cluster_object.metadata.name,
                     cluster_object.metadata.namespace, hostname, backup_file)

        try:
            backup_output = check_output(["mongodump", "--host", hostname, "--gzip", "--archive=" + backup_file])
        except CalledProcessError as err:
            raise SubprocessError("Could not backup '{}' to '{}'. Return code: {}\n stderr: '{}'\n stdout: '{}'"
                                  .format(hostname, backup_file, err.returncode, err.stderr, err.stdout))

        logging.debug("Backup output: %s", backup_output)

        self._uploadBackup(cluster_object, backup_file)
        os.remove(backup_file)

    def _uploadBackup(self, cluster_object: V1MongoClusterConfiguration, backup_file: str) -> None:
        """
        Uploads the backup file to cloud storage.
        :param cluster_object: The cluster object from the YAML file.
        :param backup_file: The location where the backup file was written to.
        """
        prefix = cluster_object.spec.backups.gcs.prefix or self.DEFAULT_BACKUP_PREFIX
        self._uploadFile(
            credentials=self._getCredentials(cluster_object),
            bucket_name=cluster_object.spec.backups.gcs.bucket,
            key="{}/{}".format(prefix, backup_file.replace("/tmp/", "")),
            file_name=backup_file
        )

    def _getCredentials(self, cluster_object: V1MongoClusterConfiguration) -> dict:
        """
        Retrieves the storage credentials for the given cluster object from the Kubernetes secret as specified in the
        cluster object.
        :param cluster_object: The cluster object from the YAML file.
        :return: The credentials dictionary.
        """
        secret_key = cluster_object.spec.backups.gcs.service_account.secret_key_ref
        secret = self.kubernetes_service.getSecret(secret_key.name, cluster_object.metadata.namespace)
        credentials_encoded = secret.data[secret_key.key]
        credentials_json = b64decode(credentials_encoded)
        return json.loads(credentials_json)

    @staticmethod
    def _uploadFile(credentials: dict, bucket_name: str, key: str, file_name: str) -> None:
        """
        Uploads a file to cloud storage.
        :param credentials: The Google cloud storage service credentials retrieved from the Kubernetes secret.
        :param bucket_name: The name of the bucket.
        :param key: The key to save the file in the cloud storage.
        :param file_name: The local file that will be uploaded.
        """
        credentials = ServiceCredentials.from_service_account_info(credentials)
        gcs_client = StorageClient(credentials.project_id, credentials)
        bucket = gcs_client.bucket(bucket_name)
        bucket.blob(key).upload_from_filename(file_name)
        logging.info("Backup uploaded to gcs://%s/%s", bucket_name, key)
