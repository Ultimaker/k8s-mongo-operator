# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import threading
from unittest import TestCase
from unittest.mock import patch, call

from kubernetes.client.rest import ApiException

from mongoOperator.managers.EventManager import EventManager
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from tests.test_utils import getExampleClusterDefinition


class TestEventManager(TestCase):
    maxDiff = None
    
    def setUp(self):
        self.shutdown_event = threading.Event()
        self.manager = EventManager(self.shutdown_event, sleep_seconds=0.01)
        self.cluster_dict = getExampleClusterDefinition()
        self.cluster_example = V1MongoClusterConfiguration(**self.cluster_dict)

    @patch("mongoOperator.managers.EventManager.EventManager.event_watcher")
    def test_run(self, watcher_mock):
        watcher_mock.stream.side_effect = lambda func, _request_timeout: self.shutdown_event.set()
        self.manager.run()
        expected_calls = [
            call.stream(EventManager.kubernetes_service.listMongoObjects, _request_timeout = 0.01),
            call.stop(),
        ]
        self.assertEquals(expected_calls, watcher_mock.mock_calls)

    @patch("mongoOperator.managers.EventManager.EventManager.kubernetes_service")
    @patch("kubernetes.watch.watch.Watch.stream")
    def test__execute(self, stream_mock, svc_mock):
        stream_mock.return_value = [
            {"type": "ADDED", "object": self.cluster_dict},
            {"type": "MODIFIED", "object": self.cluster_dict},
            {"type": "DELETED", "object": self.cluster_dict}
        ]
        self.manager.execute()
        name = self.cluster_dict["metadata"]["name"]
        namespace = self.cluster_dict["metadata"]["namespace"]
        expected_calls = [
            call.createOperatorAdminSecret(self.cluster_example),
            call.createService(self.cluster_example),
            call.createStatefulSet(self.cluster_example),
            call.updateService(self.cluster_example),
            call.updateStatefulSet(self.cluster_example),
            call.deleteStatefulSet(name, namespace),
            call.deleteService(name, namespace),
            call.deleteOperatorAdminSecret(name, namespace),
        ]
        self.assertEquals(expected_calls, svc_mock.mock_calls)

    @patch("mongoOperator.managers.EventManager.EventManager._add")
    def test__processEvent_add(self, mock_add_handler):
        self.manager._processEvent({
            "type": "ADDED",
            "object": self.cluster_dict,
        })
        mock_add_handler.assert_called_once_with(self.cluster_example)

    @patch("mongoOperator.managers.EventManager.EventManager._update")
    def test__processEvent_update(self, mock_update_handler):
        self.manager._processEvent({
            "type": "MODIFIED",
            "object": self.cluster_dict,
        })
        mock_update_handler.assert_called_once_with(self.cluster_example)

    @patch("mongoOperator.managers.EventManager.EventManager._delete")
    def test__processEvent_delete(self, mock_delete_handler):
        self.manager._processEvent({
            "type": "DELETED",
            "object": self.cluster_dict,
        })
        mock_delete_handler.assert_called_once_with(self.cluster_example)

    @patch("mongoOperator.managers.EventManager.EventManager.kubernetes_service")
    def test__processEvent_unknown(self, service_mock):
        self.manager._processEvent({
            "type": "UNKNOWN",
            "object": self.cluster_dict,
        })
        self.assertEquals([], service_mock.mock_calls)

    @patch("mongoOperator.managers.EventManager.EventManager.kubernetes_service")
    def test__processEvent_malformed(self, service_mock):
        self.manager._processEvent({"type": "ADDED"})
        self.assertEquals([], service_mock.mock_calls)

    @patch("mongoOperator.managers.EventManager.EventManager.kubernetes_service")
    def test__processEvent_api_exception(self, svc_mock):
        svc_mock.createOperatorAdminSecret.side_effect = ApiException
        with self.assertRaises(ApiException):
            self.manager._processEvent({"type": "ADDED", "object": self.cluster_dict})
