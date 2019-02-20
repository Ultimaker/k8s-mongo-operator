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
    @patch("mongoOperator.MongoOperator.ClusterManager")
    def test_run(self, checker_mock, sleep_mock):
        checker_mock.return_value.collectGarbage.side_effect = None, Exception()  # break the 3rd run

        operator = MongoOperator(sleep_per_run=0.01)
        with self.assertRaises(Exception):
            operator.run_forever()

        expected_calls = [
            call(),
            call().checkExistingClusters(), call().collectGarbage(),
            call().checkExistingClusters(), call().collectGarbage(),
        ]
        self.assertEqual(expected_calls, checker_mock.mock_calls)
        self.assertEqual([call(0.01)], sleep_mock.mock_calls)

    @patch("mongoOperator.MongoOperator.sleep")
    @patch("mongoOperator.MongoOperator.ClusterManager")
    def test_run_with_interrupt(self, checker_mock, sleep_mock):
        sleep_mock.side_effect = None, KeyboardInterrupt  # we force stop on the 2nd run

        operator = MongoOperator(sleep_per_run=0.01)
        operator.run_forever()

        expected_calls = [
            call(),
            call().checkExistingClusters(), call().collectGarbage(),
            call().checkExistingClusters(), call().collectGarbage(),
        ]
        self.assertEqual(expected_calls, checker_mock.mock_calls)
        self.assertEqual([call(0.01), call(0.01)], sleep_mock.mock_calls)
