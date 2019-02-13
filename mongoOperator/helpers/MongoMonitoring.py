# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import Callable

from pymongo import monitoring
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration

import logging


class CommandLogger(monitoring.CommandListener):

    def started(self, event):
        logging.debug("Command {0.command_name} with request id "
                      "{0.request_id} started on server "
                      "{0.connection_id}".format(event))

    def succeeded(self, event):
        logging.debug("Command {0.command_name} with request id "
                      "{0.request_id} on server {0.connection_id} "
                      "succeeded in {0.duration_micros} "
                      "microseconds".format(event))

    def failed(self, event):
        logging.debug("Command {0.command_name} with request id "
                      "{0.request_id} on server {0.connection_id} "
                      "failed in {0.duration_micros} "
                      "microseconds".format(event))


class ServerLogger(monitoring.ServerListener):

    def opened(self, event):
        logging.debug("Server {0.server_address} added to topology "
                      "{0.topology_id}".format(event))

    def description_changed(self, event):
        previous_server_type = event.previous_description.server_type
        new_server_type = event.new_description.server_type
        if new_server_type != previous_server_type:
            # server_type_name was added in PyMongo 3.4
            logging.debug(
                "Server {0.server_address} changed type from "
                "{0.previous_description.server_type_name} to "
                "{0.new_description.server_type_name}".format(event))

    def closed(self, event):
        logging.debug("Server {0.server_address} removed from topology "
                      "{0.topology_id}".format(event))


class HeartbeatLogger(monitoring.ServerHeartbeatListener):
    def __init__(self, cluster_object, all_hosts_ready_callback: Callable[[V1MongoClusterConfiguration], None]) -> None:
        self._cluster_object = cluster_object
        self._expected_host_count = cluster_object.spec.mongodb.replicas
        self._hosts = {}
        self._all_hosts_ready_callback = all_hosts_ready_callback
        self._callback_executed = False

    def started(self, event):
        logging.debug("Heartbeat sent to server "
                      "{0.connection_id}".format(event))
        self._hosts[event.connection_id] = 0

    def succeeded(self, event):
        # The reply.document attribute was added in PyMongo 3.4.
        logging.debug("Heartbeat to server {0.connection_id} "
                      "succeeded with reply "
                      "{0.reply.document}".format(event))
        self._hosts[event.connection_id] = 1

        if len(list(filter(lambda x: self._hosts[x] == 1, self._hosts))) == self._expected_host_count:
            if not self._callback_executed and "info" in event.reply.document and event.reply.document["info"] == \
                    "Does not have a valid replica set config":
                self._all_hosts_ready_callback(self._cluster_object)
                self._callback_executed = True

    def failed(self, event):
        logging.warning("Heartbeat to server {0.connection_id} "
                        "failed with error {0.reply}".format(event))
        self._hosts[event.connection_id] = -1


class TopologyLogger(monitoring.TopologyListener):

    def __init__(self, cluster_object, replica_set_ready_callback: Callable[[V1MongoClusterConfiguration], None]) -> None:
        self._cluster_object = cluster_object
        self._replica_set_ready_callback = replica_set_ready_callback

    def opened(self, event):
        logging.debug("Topology with id {0.topology_id} "
                      "opened".format(event))

    def description_changed(self, event):
        logging.debug("Topology description updated for "
                      "topology id {0.topology_id}".format(event))
        previous_topology_type = event.previous_description.topology_type
        new_topology_type = event.new_description.topology_type
        if new_topology_type != previous_topology_type:
            # topology_type_name was added in PyMongo 3.4
            logging.debug(
                "Topology {0.topology_id} changed type from "
                "{0.previous_description.topology_type_name} to "
                "{0.new_description.topology_type_name}".format(event))
        # The has_writable_server and has_readable_server methods
        # were added in PyMongo 3.4.
        if not event.new_description.has_writable_server():
            logging.debug("No writable servers available.")
        if not event.new_description.has_readable_server():
            logging.debug("No readable servers available.")

        if event.new_description.has_writable_server():
            self._replica_set_ready_callback(self._cluster_object)

    def closed(self, event):
        logging.debug("Topology with id {0.topology_id} "
                      "closed".format(event))

