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

from mongoOperator.helpers.RestoreHelper import RestoreHelper
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from tests.test_utils import getExampleClusterDefinitionWithRestore


class TestRestoreHelper(TestCase):
    def setUp(self):
        self.cluster_dict = getExampleClusterDefinitionWithRestore()
        self.cluster_object = V1MongoClusterConfiguration(**self.cluster_dict)
        self.kubernetes_service = MagicMock()
        self.restore_helper = RestoreHelper(self.kubernetes_service)

        self.dummy_credentials = b64encode(json.dumps({"user": "password"}).encode())
        self.kubernetes_service.getSecret.return_value = V1Secret(data={"json": self.dummy_credentials})

    @patch("mongoOperator.helpers.RestoreHelper.StorageClient")
    @patch("mongoOperator.helpers.RestoreHelper.ServiceCredentials")
    @patch("mongoOperator.helpers.RestoreHelper.RestoreHelper.restore")
    def test_restoreIfNeeded(self, restore_mock, gcs_service_mock, storage_mock):
        class MockBlob:
            name = 'somebackupfile.gz'
        #storage_mock.get_bucket.return_value = "foo"
        storage_mock.get_bucket.return_value.list_blobs.return_value = [MockBlob()]

        self.restore_helper.restoreIfNeeded(self.cluster_object)

        restore_mock.assert_called_once_with(self.cluster_object, 'somebackupfile.gz')

        storage_mock.bucket.assert_called_once_with('ultimaker-mongo-backups')

    @patch("mongoOperator.helpers.RestoreHelper.os")
    @patch("mongoOperator.helpers.RestoreHelper.StorageClient")
    @patch("mongoOperator.helpers.RestoreHelper.ServiceCredentials")
    @patch("mongoOperator.helpers.RestoreHelper.check_output")
    def test_restore(self, subprocess_mock, gcs_service_mock, storage_mock, os_mock):
        current_date = datetime(2018, 2, 28, 14, 0, 0)
        expected_backup_name = "mongodb-backup-default-mongo-cluster-2018-02-28_140000.archive.gz"

        self.restore_helper.restore(self.cluster_object, expected_backup_name)

        self.assertEqual([call.getSecret('storage-serviceaccount', 'default')], self.kubernetes_service.mock_calls)

        subprocess_mock.assert_called_once_with([
            'mongorestore', '--host', 'mongo-cluster-2.mongo-cluster.default.svc.cluster.local', '--gzip',
            '--archive', '/tmp/' + expected_backup_name
        ])

        expected_service_call = call.from_service_account_info({'user': 'password'})
        self.assertEqual([expected_service_call], gcs_service_mock.mock_calls)

        expected_storage_calls = [
            call(gcs_service_mock.from_service_account_info.return_value.project_id,
                 gcs_service_mock.from_service_account_info.return_value),
            call().bucket('ultimaker-mongo-backups'),
            call().bucket().blob('test-backups/' + expected_backup_name),
            call().bucket().blob().download_to_filename('/tmp/' + expected_backup_name),
        ]
        self.assertEqual(expected_storage_calls, storage_mock.mock_calls)

        expected_os_call = call.remove('/tmp/' + expected_backup_name)
        self.assertEqual([expected_os_call], os_mock.mock_calls)

    @patch("mongoOperator.helpers.RestoreHelper.os")
    @patch("mongoOperator.helpers.RestoreHelper.StorageClient")
    @patch("mongoOperator.helpers.RestoreHelper.ServiceCredentials")
    @patch("mongoOperator.helpers.RestoreHelper.check_output")
    def test_restore_mongo_error(self, subprocess_mock, gcs_service_mock, storage_mock, os_mock):
        subprocess_mock.side_effect = CalledProcessError(3, "cmd", "output", "error")
        expected_backup_name = "mongodb-backup-default-mongo-cluster-2018-02-28_140000.archive.gz"                      

        current_date = datetime(2018, 2, 28, 14, 0, 0)

        with self.assertRaises(SubprocessError) as context:
            self.restore_helper.restore(self.cluster_object, expected_backup_name)

        self.assertEqual("Could not restore "
                         "'" + expected_backup_name + "'"
                         " to "
                         "'mongo-cluster-2.mongo-cluster.default.svc.cluster.local'. "
                         "Return code: 3\n stderr: 'error'\n stdout: 'output'",
                         str(context.exception))

        self.assertEqual(1, subprocess_mock.call_count)

    @patch("mongoOperator.helpers.RestoreHelper.check_output")
    def test_restore_gcs_bad_credentials(self, subprocess_mock):
        expected_backup_name = "mongodb-backup-default-mongo-cluster-2018-02-28_140000.archive.gz"                      
        with self.assertRaises(ValueError) as context:
            self.restore_helper.restore(self.cluster_object, expected_backup_name)
        self.assertIn("Service account info was not in the expected format", str(context.exception))
