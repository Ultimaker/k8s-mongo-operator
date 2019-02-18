# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from typing import Callable

from pymongo.monitoring import TopologyListener as MongoTopologyListener, TopologyOpenedEvent,\
    TopologyDescriptionChangedEvent, TopologyClosedEvent

from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration


class TopologyListener(MongoTopologyListener):
    """ Listener for Mongo cluster topology events. """

    def __init__(self, cluster_object: V1MongoClusterConfiguration,
                 replica_set_ready_callback: Callable[[V1MongoClusterConfiguration], None]) -> None:
        super().__init__()

        self._cluster_object: V1MongoClusterConfiguration = cluster_object
        self._replica_set_ready_callback: Callable[[V1MongoClusterConfiguration], None] = replica_set_ready_callback

    def opened(self, event: TopologyOpenedEvent) -> None:
        """
        When a topology opened.
        :param event: The event.
        """
        logging.debug("Topology with id %s opened", event.topology_id)

    def description_changed(self, event: TopologyDescriptionChangedEvent) -> None:
        """
        When the description of a topology changed.
        :param event: The event.
        """
        logging.debug("Topology description updated for topology id %s", event.topology_id)

        previous_topology_type = event.previous_description.topology_type
        new_topology_type = event.new_description.topology_type
        if new_topology_type != previous_topology_type:
            # topology_type_name was added in PyMongo 3.4
            logging.debug("Topology %s changed type from %s to %s", event.topology_id,
                          event.previous_description.topology_type_name,
                          event.new_description.topology_type_name)

        # The has_writable_server and has_readable_server methods were added in PyMongo 3.4.
        if not event.new_description.has_writable_server():
            logging.info("No writable servers available.")
        if not event.new_description.has_readable_server():
            logging.info("No readable servers available.")

        if not event.new_description.has_writable_server():
            # We cannot write to a server yet, so we cannot initiate the replica set via the callback.
            return

        self._replica_set_ready_callback(self._cluster_object)

    def closed(self, event: TopologyClosedEvent) -> None:
        """
        When topology was closed.
        :param event: The event.
        """
        logging.debug("Topology with id %s closed", event.topology_id)
