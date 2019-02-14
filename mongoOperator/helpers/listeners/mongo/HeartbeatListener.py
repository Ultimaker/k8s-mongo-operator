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
        logging.debug("Heartbeat sent to server {0.connection_id}".format(event))
        self._hosts[event.connection_id] = 0

    def succeeded(self, event: ServerHeartbeatSucceededEvent) -> None:
        """
        When the heartbeat arrived.
        :param event: The event.
        """
        # The reply.document attribute was added in PyMongo 3.4.
        logging.debug("Heartbeat to server {0.connection_id} succeeded with reply {0.reply.document}".format(event))
        self._hosts[event.connection_id] = 1
        
        if self._callback_executed:
            # The callback was already executed so we don't have to again.
            logging.debug("The callback was already executed")
            return

        host_count_found = len(list(filter(lambda x: self._hosts[x] == 1, self._hosts)))
        if self._expected_host_count != host_count_found:
            # The amount of returned hosts was different than expected.
            logging.debug("The host count did not match the expected host count: {} found, {} expected".format(
                host_count_found, self._expected_host_count
            ))
            return

        if "info" in event.reply.document and event.reply.document["info"] == self.INVALID_REPLICA_SET_CONFIG:
            # The reply indicated that the replica set config was not correct.
            logging.debug("The replica set config was not correct: {}".format(repr(event.reply)))
            return

        self._all_hosts_ready_callback(self._cluster_object)
        self._callback_executed = True

    def failed(self, event: ServerHeartbeatFailedEvent) -> None:
        """
        When the heartbeat did not arrive.
        :param event: The event.
        """
        logging.warning("Heartbeat to server {0.connection_id} failed with error {0.reply}".format(event))
        self._hosts[event.connection_id] = -1
