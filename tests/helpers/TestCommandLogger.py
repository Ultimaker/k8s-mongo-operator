# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import cast
from unittest import TestCase

from pymongo.monitoring import CommandStartedEvent, CommandFailedEvent, CommandSucceededEvent

from mongoOperator.helpers.listeners.mongo.CommandLogger import CommandLogger


class CommandEventMock:
    """ Mock implementation of a CommandEvent. """
    command_name = "foo"
    request_id = 1
    connection_id = 1
    duration_micros = 10000
    

class TestCommandLogger(TestCase):
    command_logger = CommandLogger()

    def test_started(self):
        self.command_logger.started(event = cast(CommandStartedEvent, CommandEventMock()))
        
    def test_succeeded(self):
        self.command_logger.succeeded(event = cast(CommandSucceededEvent, CommandEventMock()))
        
    def test_failed(self):
        self.command_logger.failed(event = cast(CommandFailedEvent, CommandEventMock()))
