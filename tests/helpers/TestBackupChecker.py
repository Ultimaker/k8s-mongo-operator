# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import json
from base64 import b64encode

from kubernetes.client import V1Secret
from subprocess import CalledProcessError, SubprocessError

from datetime import datetime
from unittest import TestCase
from unittest.mock import MagicMock, patch, call

from mongoOperator.helpers.BackupHelper import BackupHelper
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from tests.test_utils import getExampleClusterDefinition


class TestBackupChecker(TestCase):
    def setUp(self):
        self.cluster_dict = getExampleClusterDefinition()
        self.cluster_object = V1MongoClusterConfiguration(**self.cluster_dict)
        self.kubernetes_service = MagicMock()
        self.checker = BackupHelper(self.kubernetes_service)

        self.dummy_credentials = b64encode(json.dumps({"user": "password"}).encode())
        self.kubernetes_service.getSecret.return_value = V1Secret(data={"json": self.dummy_credentials})

    def test__utcNow(self):
        before = datetime.utcnow()
        actual = self.checker._utcNow()
        after = datetime.utcnow()
        self.assertTrue(before <= actual <= after)

    @patch("mongoOperator.helpers.BackupHelper.BackupHelper.backup")
    def test_backupIfNeeded_check_if_needed(self, backup_mock):
        # this backup is executed every hour at 0 minutes.
        self.assertEqual("0 * * * *", self.cluster_object.spec.backups.cron)

        key = ("mongo-cluster", self.cluster_object.metadata.namespace)

        expected_calls = []
        current_date = datetime(2018, 2, 28, 12, 30, 0)

        with patch("mongoOperator.helpers.BackupHelper.BackupHelper._utcNow", lambda _: current_date):
            # on the first run, it should backup regardless of the time.
            self.checker.backupIfNeeded(self.cluster_object)
            expected_calls.append(call(self.cluster_object, current_date))
            self.assertEqual(expected_calls, backup_mock.mock_calls)
            self.assertEqual({key: current_date}, self.checker._last_backups)

            # 20 minutes later, no backup needed.
            current_date = datetime(2018, 2, 28, 12, 50, 0)
            self.checker.backupIfNeeded(self.cluster_object)
            self.assertEqual(expected_calls, backup_mock.mock_calls)
            self.assertEqual({key: datetime(2018, 2, 28, 12, 30, 0)}, self.checker._last_backups)

            # another 15 minutes later, backup needed.
            current_date = datetime(2018, 2, 28, 13, 5, 0)
            self.checker.backupIfNeeded(self.cluster_object)
            expected_calls.append(call(self.cluster_object, current_date))
            self.assertEqual(expected_calls, backup_mock.mock_calls)
            self.assertEqual({key: current_date}, self.checker._last_backups)

            # 45 minutes later, no backup needed.
            current_date = datetime(2018, 2, 28, 13, 50, 0)
            self.checker.backupIfNeeded(self.cluster_object)
            self.assertEqual(expected_calls, backup_mock.mock_calls)
            self.assertEqual({key: datetime(2018, 2, 28, 13, 5, 0)}, self.checker._last_backups)

            # another 15 minutes later, backup needed.
            current_date = datetime(2018, 2, 28, 14, 0, 0)
            self.checker.backupIfNeeded(self.cluster_object)
            expected_calls.append(call(self.cluster_object, current_date))
            self.assertEqual(expected_calls, backup_mock.mock_calls)
            self.assertEqual({key: current_date}, self.checker._last_backups)

    @patch("mongoOperator.helpers.BackupHelper.os")
    @patch("mongoOperator.helpers.BackupHelper.StorageClient")
    @patch("mongoOperator.helpers.BackupHelper.ServiceCredentials")
    @patch("mongoOperator.helpers.BackupHelper.check_output")
    def test_backup(self, subprocess_mock, gcs_service_mock, storage_mock, os_mock):
        current_date = datetime(2018, 2, 28, 14, 0, 0)
        expected_backup_name = "mongodb-backup-mongo-operator-cluster-mongo-cluster-2018-02-28_140000.archive.gz"

        self.checker.backup(self.cluster_object, current_date)

        self.assertEqual([call.getSecret("storage-serviceaccount", "mongo-operator-cluster")],
                         self.kubernetes_service.mock_calls)

        subprocess_mock.assert_called_once_with([
            "mongodump", "--host", "mongo-cluster-2.mongo-cluster.mongo-operator-cluster.svc.cluster.local", "--gzip",
            "--archive=/tmp/" + expected_backup_name
        ])

        expected_service_call = call.from_service_account_info({"user": "password"})
        self.assertEqual([expected_service_call], gcs_service_mock.mock_calls)

        expected_storage_calls = [
            call(gcs_service_mock.from_service_account_info.return_value.project_id,
                 gcs_service_mock.from_service_account_info.return_value),
            call().bucket("ultimaker-mongo-backups"),
            call().bucket().blob("test-backups/" + expected_backup_name),
            call().bucket().blob().upload_from_filename("/tmp/" + expected_backup_name),
        ]
        self.assertEqual(expected_storage_calls, storage_mock.mock_calls)

        expected_os_call = call.remove("/tmp/" + expected_backup_name)
        self.assertEqual([expected_os_call], os_mock.mock_calls)

    @patch("mongoOperator.helpers.BackupHelper.check_output")
    def test_backup_mongo_error(self, subprocess_mock):
        subprocess_mock.side_effect = CalledProcessError(3, "cmd", "output", "error")
        current_date = datetime(2018, 2, 28, 14, 0, 0)

        with self.assertRaises(SubprocessError) as context:
            self.checker.backup(self.cluster_object, current_date)

        self.assertEqual("Could not backup 'mongo-cluster-2.mongo-cluster.mongo-operator-cluster.svc.cluster.local' to "
                         "'/tmp/mongodb-backup-mongo-operator-cluster-mongo-cluster-2018-02-28_140000.archive.gz'. "
                         "Return code: 3\n stderr: 'error'\n stdout: 'output'",
                         str(context.exception))
        self.assertEqual(1, subprocess_mock.call_count)

    @patch("mongoOperator.helpers.BackupHelper.check_output")
    def test_backup_gcs_bad_credentials(self, subprocess_mock):
        current_date = datetime(2018, 2, 28, 14, 0, 0)
        with self.assertRaises(ValueError) as context:
            self.checker.backup(self.cluster_object, current_date)
        self.assertIn("Service account info was not in the expected format", str(context.exception))
        self.assertEqual(1, subprocess_mock.call_count)
