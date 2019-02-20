# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from typing import Callable, Dict

from pymongo.monitoring import ServerHeartbeatListener, ServerHeartbeatStartedEvent, ServerHeartbeatSucceededEvent,\
    ServerHeartbeatFailedEvent

from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration


class HeartbeatListener(ServerHeartbeatListener):
    """ A listener for Mongo server heartbeats. """

    INVALID_REPLICA_SET_CONFIG = "Does not have a valid replica set config"

    def __init__(self, cluster_object: V1MongoClusterConfiguration,
                 all_hosts_ready_callback: Callable[[V1MongoClusterConfiguration], None]) -> None:
        super().__init__()
        self._cluster_object: V1MongoClusterConfiguration = cluster_object
        self._expected_host_count: int = cluster_object.spec.mongodb.replicas
        self._hosts: Dict[str, int] = {}
        self._all_hosts_ready_callback: Callable[[V1MongoClusterConfiguration], None] = all_hosts_ready_callback
        self._callback_executed = False

    def started(self, event: ServerHeartbeatStartedEvent) -> None:
        """
        When the heartbeat was sent.
        :param event: The event.
        """
        logging.debug("Heartbeat sent to server %s", event.connection_id)
        self._hosts[event.connection_id] = 0

    def succeeded(self, event: ServerHeartbeatSucceededEvent) -> None:
        """
        When the heartbeat arrived.
        :param event: The event.
        """
        logging.debug("Heartbeat to server %s succeeded with reply %s", event.connection_id, event.reply.document)
        self._hosts[event.connection_id] = 1

        if self._callback_executed:
            # The callback was already executed so we don't have to again.
            logging.debug("The callback was already executed")
            return

        host_count_found = len(list(filter(lambda x: self._hosts[x] == 1, self._hosts)))
        if self._expected_host_count != host_count_found:
            # The amount of returned hosts was different than expected.
            logging.debug("The host count did not match the expected host count: %s found, %s expected",
                          host_count_found, self._expected_host_count)
            return

        # Only execute the callback on the first host
        if list(self._hosts.keys())[0] == event.connection_id:
            self._all_hosts_ready_callback(self._cluster_object)
            self._callback_executed = True

    def failed(self, event: ServerHeartbeatFailedEvent) -> None:
        """
        When the heartbeat did not arrive.
        :param event: The event.
        """
        logging.warning("Heartbeat to server %s failed with error %s",
                        event.connection_id, event.reply)
        self._hosts[event.connection_id] = -1
