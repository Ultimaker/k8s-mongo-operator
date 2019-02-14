# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging
import os
from base64 import b64decode
from subprocess import check_output, CalledProcessError, SubprocessError

from time import sleep
from google.cloud.storage import Client as StorageClient
from google.oauth2.service_account import Credentials as ServiceCredentials

from mongoOperator.helpers.MongoResources import MongoResources
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService


class RestoreHelper:
    """
    Class responsible for handling the Restores for the Mongo cluster.
    """
    DEFAULT_BACKUP_PREFIX = "backups"
    BACKUP_FILE_FORMAT = "mongodb-backup-{namespace}-{name}-{date}.archive.gz"
    RESTORE_RETRIES = 4
    RESTORE_WAIT = 15.0

    def __init__(self, kubernetes_service: KubernetesService):
        """
        :param kubernetes_service: The kubernetes service.
        """
        self.kubernetes_service = kubernetes_service

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

    def getLastBackup(self, cluster_object: V1MongoClusterConfiguration) -> str:
        """
        Returns the filename of the last backup file in the bucket.
        :param cluster_object: The cluster object from the YAML file.
        :return: String containing the filename of the last backup.
        """
        prefix = cluster_object.spec.backups.gcs.prefix or self.DEFAULT_BACKUP_PREFIX
        bucket_name = cluster_object.spec.backups.gcs.restore_bucket if cluster_object.spec.backups.gcs.restore_bucket \
            else cluster_object.spec.backups.gcs.bucket
        return self._lastBackupFile(
            credentials=self._getCredentials(cluster_object),
            bucket_name=bucket_name,
            key="{}/".format(prefix)
        )

    @staticmethod
    def _lastBackupFile(credentials: dict, bucket_name: str, key: str) -> str:
        """
        Gets the name of the last backup file in the bucket.
        :param credentials: The Google cloud storage service credentials retrieved from the Kubernetes secret.
        :param bucket_name: The name of the bucket.
        :param key: The prefix of tha backups
        :return: The location of the last backup file.
        """
        credentials = ServiceCredentials.from_service_account_info(credentials)
        gcs_client = StorageClient(credentials.project_id, credentials)
        bucket = gcs_client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=key)

        last_blob = None
        for blob in blobs:
            logging.info("Found backup file '%s' in bucket '%s'", blob.name, bucket_name)
            if last_blob is None or blob.time_created > last_blob.time_created:
                last_blob = blob

        logging.info("Returning backup file %s", last_blob.name.replace(key, ""))
        return last_blob.name.replace(key, "") if last_blob else None

    def restoreIfNeeded(self, cluster_object: V1MongoClusterConfiguration) -> bool:
        """
        Checks whether a restore is requested for the cluster, looking up the restore file if
        necessary.
        :param cluster_object: The cluster object from the YAML file.
        :return: Whether a restore was executed or not.
        """
        if cluster_object.spec.backups.gcs.restore_from is not None:
            backup_file = cluster_object.spec.backups.gcs.restore_from
            if backup_file == "latest":
                backup_file = self.getLastBackup(cluster_object)

            logging.info("Attempting to restore file %s to cluster %s @ ns/%s.", backup_file,
                         cluster_object.metadata.name, cluster_object.metadata.namespace)

            self.restore(cluster_object, backup_file)
            return True

        return False

    def restore(self, cluster_object: V1MongoClusterConfiguration, backup_file: str) -> bool:
        """
        Attempts to restore the latest backup in the specified location to the given cluster.
        Creates a new backup for the given cluster saving it in the cloud storage.
        :param cluster_object: The cluster object from the YAML file.
        :param backup_file: The filename of the backup we want to restore.
        """
        hostnames = MongoResources.getMemberHostnames(cluster_object)

        logging.info("Restoring backup file %s to cluster %s @ ns/%s.", backup_file, cluster_object.metadata.name,
                     cluster_object.metadata.namespace)

        # Download the backup file from the bucket
        downloaded_file = self._downloadBackup(cluster_object, backup_file)

        for _ in range(self.RESTORE_RETRIES):
            # Wait for the replica set to become ready
            try:
                logging.info("Running mongorestore --host %s --gzip --archive=%s", ",".join(hostnames), downloaded_file)
                restore_output = check_output(["mongorestore", "--host", ",".join(hostnames), "--gzip",
                                               "--archive=" + downloaded_file])
                logging.info("Restore output: %s", restore_output)
                os.remove(downloaded_file)
                return True
            except CalledProcessError as err:
                logging.error("Could not restore '%s', attempt %d. Return code: %s stderr: '%s' stdout: '%s'",
                              backup_file, _, err.returncode, err.stderr, err.stdout)
                sleep(self.RESTORE_WAIT)
        raise SubprocessError("Could not restore '{}' after {} retries!".format(backup_file, self.RESTORE_RETRIES))

    def _downloadBackup(self, cluster_object: V1MongoClusterConfiguration, backup_file: str) -> str:
        """
        Downloads the backup file from cloud storage.
        :param cluster_object: The cluster object from the YAML file.
        :param backup_file: The file name of the backup to download.
        :return: The location of the downloaded file.
        """
        prefix = cluster_object.spec.backups.gcs.prefix or self.DEFAULT_BACKUP_PREFIX
        restore_bucket = cluster_object.spec.backups.gcs.restore_bucket or cluster_object.spec.backups.gcs.bucket
        return self._downloadFile(
            credentials=self._getCredentials(cluster_object),
            bucket_name=restore_bucket,
            key="{}/{}".format(prefix, backup_file),
            file_name="/tmp/" + backup_file
        )

    @staticmethod
    def _downloadFile(credentials: dict, bucket_name: str, key: str, file_name: str) -> str:
        """
        Downloads a file from cloud storage.
        :param credentials: The Google cloud storage service credentials retrieved from the Kubernetes secret.
        :param bucket_name: The name of the bucket.
        :param key: The key to download the file from the cloud storage.
        :param file_name: The file that will be downloaded.
        :return: The location of the downloaded file.
        """
        credentials = ServiceCredentials.from_service_account_info(credentials)
        gcs_client = StorageClient(credentials.project_id, credentials)
        bucket = gcs_client.get_bucket(bucket_name)
        logging.info("Going to download gcs://%s/%s", bucket_name, key)

        bucket.blob(key).download_to_filename(file_name)

        logging.info("Backup gcs://%s/%s downloaded to %s", bucket_name, key, file_name)
        return file_name
