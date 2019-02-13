# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-

from mongoOperator.helpers.MongoMonitoring import CommandLogger, TopologyLogger, ServerLogger, HeartbeatLogger

from unittest import TestCase
from unittest.mock import MagicMock, patch, call


class CommandEventMock:
    """
    Mock implementation of a CommandEvent.
    """
    command_name = "foo"
    request_id = 1
    connection_id = 1
    duration_micros = 10000


class ServerDescriptionEventMock:
    server_type = "foo"
    server_type_name = "foo"


class ServerEventMock:
    """
    Mock implementation of a ServerEvent.
    """
    server_address = "localhost"
    topology_id = 1
    previous_description = ServerDescriptionEventMock()
    new_description = ServerDescriptionEventMock()


class TestRestoreHelper(TestCase):

    def setUp(self):
        return

    def test_commandLogger(self):
        commandlogger = CommandLogger()
        commandlogger.started(event=CommandEventMock())
        commandlogger.succeeded(event=CommandEventMock())
        commandlogger.failed(event=CommandEventMock())

    def test_serverLogger(self):
        serverlogger = ServerLogger()
        serverlogger.opened(event=ServerEventMock())
        serverlogger.description_changed(event=ServerEventMock())
