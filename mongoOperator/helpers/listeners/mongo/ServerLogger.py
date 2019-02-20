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
        logging.debug("Server %s added to topology %s", event.server_address, event.topology_id)

    def description_changed(self, event: ServerDescriptionChangedEvent) -> None:
        """
        When the description of the server changed.
        :param event: The event.
        """
        previous_server_type = event.previous_description.server_type
        new_server_type = event.new_description.server_type
        if new_server_type != previous_server_type:
            logging.debug("Server %s changed type from %s to %s", event.server_address,
                          event.previous_description.server_type_name, event.new_description.server_type_name)

    def closed(self, event: ServerClosedEvent) -> None:
        """
        When the server was removed from the network.
        :param event: The event.
        """
        logging.debug("Server %s removed from topology %s",
                      event.server_address, event.topology_id)
