# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import cast
from unittest import TestCase
from unittest.mock import Mock, MagicMock

from pymongo.monitoring import TopologyDescriptionChangedEvent, TopologyOpenedEvent, TopologyClosedEvent
from mongoOperator.helpers.listeners.mongo.TopologyListener import TopologyListener
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from tests.test_utils import getExampleClusterDefinition


class TestTopologyLogger(TestCase):
    def setUp(self):
        self.cluster_dict = getExampleClusterDefinition()
        self.cluster_object = V1MongoClusterConfiguration(**self.cluster_dict)
        self.kubernetes_service = MagicMock()
        self._onReplicaSetReadyCallback = MagicMock()

    def test_opened(self):
        topology_logger = TopologyListener(self.cluster_object,
                                           replica_set_ready_callback=self._onReplicaSetReadyCallback)

        topology_logger.opened(event=cast(TopologyOpenedEvent, Mock(spec=TopologyOpenedEvent)))

    def test_description_changed(self):
        topology_logger = TopologyListener(self.cluster_object,
                                           replica_set_ready_callback=self._onReplicaSetReadyCallback)

        topology_description_changed_event_mock = MagicMock(spec=TopologyDescriptionChangedEvent)
        topology_description_changed_event_mock.new_description.topology_type = "foo"
        topology_description_changed_event_mock.new_description.has_writable_server.return_value = False
        topology_description_changed_event_mock.new_description.has_readable_server.return_value = False
        topology_logger.description_changed(event=cast(TopologyDescriptionChangedEvent,
                                                       topology_description_changed_event_mock))

    def test_description_changed_with_callback(self):
        topology_logger = TopologyListener(self.cluster_object,
                                           replica_set_ready_callback=self._onReplicaSetReadyCallback)

        topology_description_changed_event_mock = MagicMock(spec=TopologyDescriptionChangedEvent)
        topology_description_changed_event_mock.new_description.has_writable_server.return_value = True

        topology_logger.description_changed(event=cast(TopologyDescriptionChangedEvent,
                                                       topology_description_changed_event_mock))

        self._onReplicaSetReadyCallback.assert_called_once_with(self.cluster_object)

    def test_closed(self):
        topology_logger = TopologyListener(self.cluster_object,
                                           replica_set_ready_callback=self._onReplicaSetReadyCallback)
        topology_logger.closed(event=cast(TopologyClosedEvent, Mock(spec=TopologyClosedEvent)))


