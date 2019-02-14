# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from typing import cast
from unittest import TestCase

from pymongo.monitoring import ServerOpeningEvent, ServerClosedEvent, ServerDescriptionChangedEvent

from mongoOperator.helpers.listeners.mongo.ServerLogger import ServerLogger


class ServerDescriptionEventMock:
    server_type = "foo"
    server_type_name = "foo"
    

class ServerEventMock:
    """ Mock implementation of a ServerEvent. """
    server_address = "localhost"
    topology_id = 1
    previous_description = ServerDescriptionEventMock()
    new_description = ServerDescriptionEventMock()


class TestServerLogger(TestCase):
    server_logger = ServerLogger()
    
    def test_opened(self):
        self.server_logger.opened(event=cast(ServerOpeningEvent, ServerEventMock()))
    
    def test_closed(self):
        self.server_logger.closed(event=cast(ServerClosedEvent, ServerEventMock()))
        
    def test_description_changed(self):
        serverEventMock = ServerEventMock()
        serverEventMock.new_description.server_type = "bar"
        self.server_logger.description_changed(event=cast(ServerDescriptionChangedEvent, serverEventMock))
