# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from pymongo.monitoring import ServerDescriptionChangedEvent, ServerOpeningEvent, ServerClosedEvent, ServerListener


class ServerLogger(ServerListener):
    """ A simple logger for Mongo server events in the cluster. """

    def opened(self, event: ServerOpeningEvent) -> None:
        """
        When the server was added to the network.
        :param event: The event.
        """
        logging.debug("Server {0.server_address} added to topology {0.topology_id}".format(event))

    def description_changed(self, event: ServerDescriptionChangedEvent) -> None:
        """
        When the description of the server changed.
        :param event: The event.
        """
        previous_server_type = event.previous_description.server_type
        new_server_type = event.new_description.server_type
        if new_server_type != previous_server_type:
            # server_type_name was added in PyMongo 3.4
            logging.debug(
                "Server {0.server_address} changed type from "
                "{0.previous_description.server_type_name} to "
                "{0.new_description.server_type_name}".format(event))

    def closed(self, event: ServerClosedEvent) -> None:
        """
        When the server was removed from the network.
        :param event: The event.
        """
        logging.debug("Server {0.server_address} removed from topology {0.topology_id}".format(event))
