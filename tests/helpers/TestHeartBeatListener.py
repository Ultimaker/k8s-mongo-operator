# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import cast
from unittest import TestCase
from unittest.mock import Mock, MagicMock

from pymongo.monitoring import ServerHeartbeatStartedEvent, ServerHeartbeatSucceededEvent, ServerHeartbeatFailedEvent
from mongoOperator.helpers.listeners.mongo.HeartbeatListener import HeartbeatListener
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from tests.test_utils import getExampleClusterDefinition


class TestHeartbeatLogger(TestCase):
    def setUp(self):
        self.cluster_dict = getExampleClusterDefinition()
        self.cluster_object = V1MongoClusterConfiguration(**self.cluster_dict)
        self.kubernetes_service = MagicMock()
        self._onAllHostsReadyCallback = MagicMock()

    def test_started(self):
        heartbeat_logger = HeartbeatListener(self.cluster_object,
                                             all_hosts_ready_callback=self._onAllHostsReadyCallback)

        heartbeat_logger.started(event=cast(ServerHeartbeatStartedEvent, Mock(spec=ServerHeartbeatStartedEvent)))

    def test_succeeded(self):
        heartbeat_logger = HeartbeatListener(self.cluster_object,
                                             all_hosts_ready_callback=self._onAllHostsReadyCallback)

        heartbeat_event_mock = MagicMock(spec=ServerHeartbeatSucceededEvent)
        heartbeat_event_mock.reply.document = {"info": ""}
        heartbeat_event_mock.connection_id = "host-1", "27017"

        heartbeat_logger.succeeded(event=cast(ServerHeartbeatSucceededEvent, heartbeat_event_mock))
        heartbeat_event_mock.connection_id = "host-2", "27017"
        heartbeat_logger.succeeded(event=cast(ServerHeartbeatSucceededEvent, heartbeat_event_mock))

        heartbeat_event_mock.connection_id = "host-3", "27017"
        heartbeat_logger.succeeded(event=cast(ServerHeartbeatSucceededEvent, heartbeat_event_mock))

        heartbeat_event_mock.reply.document = {"info": ""}
        heartbeat_event_mock.connection_id = "host-1", "27017"

        heartbeat_logger.succeeded(event=cast(ServerHeartbeatSucceededEvent, heartbeat_event_mock))

        self._onAllHostsReadyCallback.assert_called_once_with(self.cluster_object)

    def test_succeeded_invalid_replicaSet(self):
        heartbeat_logger = HeartbeatListener(self.cluster_object,
                                             all_hosts_ready_callback=self._onAllHostsReadyCallback)

        # Fake two already successful hosts
        heartbeat_logger._hosts = {"foo": 1, "bar": 1}

        # Call it with invalid replicaSet configuration
        heartbeat_event_mock = MagicMock(spec=ServerHeartbeatSucceededEvent)
        heartbeat_event_mock.reply.document = {"info": "Does not have a valid replica set config"}
        heartbeat_logger.succeeded(event=cast(ServerHeartbeatSucceededEvent, heartbeat_event_mock))

    def test_succeeded_already_called(self):
        heartbeat_logger = HeartbeatListener(self.cluster_object,
                                             all_hosts_ready_callback=self._onAllHostsReadyCallback)

        heartbeat_logger._callback_executed = True
        heartbeat_logger.succeeded(event=cast(ServerHeartbeatSucceededEvent, MagicMock()))

    def test_failed(self):
        heartbeat_logger = HeartbeatListener(self.cluster_object,
                                             all_hosts_ready_callback=self._onAllHostsReadyCallback)
        heartbeat_logger.failed(event=cast(ServerHeartbeatFailedEvent, Mock(spec=ServerHeartbeatFailedEvent)))
