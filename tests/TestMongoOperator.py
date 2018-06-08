# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import threading
from unittest import TestCase
from unittest.mock import patch, call

from mongoOperator.MongoOperator import MongoOperator


class TestMongoOperator(TestCase):
    maxDiff = None

    @patch("mongoOperator.MongoOperator.sleep")
    @patch("mongoOperator.MongoOperator.threading.Thread")
    def test_run(self, thread_mock, sleep_mock):
        sleep_mock.side_effect = None, KeyboardInterrupt
        thread_mock.return_value.ident = None

        operator = MongoOperator(sleep_per_manager=0.005, sleep_per_run=0.01)
        operator.run()
        expected_calls = [
            call(args=(operator._shutting_down, 0.01), name='PeriodicCheck', target=operator._startPeriodicalCheck),
            call(args=(operator._shutting_down, 0.01), name='EventListener', target=operator._startEventListener),
            call().start(), call().start(), call().start(), call().start(),
            call().join(), call().join(),
        ]
        self.assertEqual(expected_calls, thread_mock.mock_calls)

    @patch("mongoOperator.managers.PeriodicalCheckManager.PeriodicalCheckManager.run")
    def test__startPeriodicalCheck(self, run_mock):
        shutting_down_event = threading.Event()
        MongoOperator._startPeriodicalCheck(shutting_down_event, sleep_seconds=0.01)
        run_mock.assert_called_once_with()

    @patch("mongoOperator.managers.EventManager.EventManager.run")
    def test__startEventListener(self, run_mock):
        shutting_down_event = threading.Event()
        MongoOperator._startEventListener(shutting_down_event, sleep_seconds=0.01)
        run_mock.assert_called_once_with()
