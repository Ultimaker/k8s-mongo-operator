# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from pymongo.monitoring import CommandStartedEvent, CommandListener, CommandSucceededEvent, CommandFailedEvent


class CommandLogger(CommandListener):
    """ Simple logger for mongo commands being executed in the cluster. """

    def started(self, event: CommandStartedEvent) -> None:
        """
        When a command was started.
        :param event: The event.
        """
        logging.debug("Command %s with request id %s started on server %s",
                      event.command_name, event.request_id, event.connection_id)

    def succeeded(self, event: CommandSucceededEvent) -> None:
        """
        When a command succeeded.
        :param event: The event.
        """
        logging.debug("Command %s with request id %s on server %s succeeded in %s microseconds",
                      event.command_name, event.request_id, event.connection_id, event.duration_micros)

    def failed(self, event: CommandFailedEvent) -> None:
        """
        When a command failed.
        :param event: The event.
        """
        logging.debug("Command %s with request id %s on server %s failed in %s microseconds",
                      event.command_name, event.request_id, event.connection_id, event.duration_micros)
