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
        logging.debug("Command {0.command_name} with request id {0.request_id} started on server {0.connection_id}"
                      .format(event))

    def succeeded(self, event: CommandSucceededEvent) -> None:
        """
        When a command succeeded.
        :param event: The event.
        """
        logging.debug("Command {0.command_name} with request id {0.request_id} on server {0.connection_id} succeeded "
                      "in {0.duration_micros} microseconds".format(event))

    def failed(self, event: CommandFailedEvent) -> None:
        """
        When a command failed.
        :param event: The event.
        """
        logging.debug("Command {0.command_name} with request id {0.request_id} on server {0.connection_id} failed in "
                      "{0.duration_micros} microseconds".format(event))
