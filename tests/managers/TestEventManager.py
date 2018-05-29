# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import threading
from unittest import TestCase
from unittest.mock import patch

from mongoOperator.managers.EventManager import EventManager


class TestEventManager(TestCase):
    
    def setUp(self):
        self.manager = EventManager(threading.Event(), 10)

    @patch("mongoOperator.managers.EventManager.EventManager._add")
    def test_processEvent_add(self, mock_add_handler):
        fake_cluster_object = {}
        self.manager._processEvent({
            "type": "ADDED",
            "object": fake_cluster_object
        })
        mock_add_handler.assert_called_once_with(fake_cluster_object)

    @patch("mongoOperator.managers.EventManager.EventManager._update")
    def test_processEvent_update(self, mock_update_handler):
        fake_cluster_object = {}
        self.manager._processEvent({
            "type": "MODIFIED",
            "object": fake_cluster_object
        })
        mock_update_handler.assert_called_once_with(fake_cluster_object)

    @patch("mongoOperator.managers.EventManager.EventManager._delete")
    def test_processEvent_delete(self, mock_delete_handler):
        fake_cluster_object = {}
        self.manager._processEvent({
            "type": "DELETED",
            "object": fake_cluster_object
        })
        mock_delete_handler.assert_called_once_with(fake_cluster_object)

    @patch("mongoOperator.managers.EventManager.EventManager._add")
    @patch("mongoOperator.managers.EventManager.EventManager._update")
    @patch("mongoOperator.managers.EventManager.EventManager._delete")
    def test_processEvent_unknown(self, mock_delete_handler, mock_update_handler, mock_add_handler):
        self.manager._processEvent({
            "type": "UNKNOWN",
            "object": {}
        })
        assert mock_add_handler.called is False
        assert mock_update_handler.called is False
        assert mock_delete_handler.called is False
