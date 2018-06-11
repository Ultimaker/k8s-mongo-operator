# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase
from unittest.mock import MagicMock

from kubernetes.client.rest import ApiException

from mongoOperator.helpers.IgnoreIfExists import IgnoreIfExists


class TestIgnoreIfExists(TestCase):
    def test_no_error(self):
        with IgnoreIfExists():
            pass

    def test_no_api_exception(self):
        with self.assertRaises(ValueError):
            with IgnoreIfExists():
                raise ValueError()

    def test_api_exception_code_not_409(self):
        with self.assertRaises(ApiException):
            with IgnoreIfExists():
                raise ApiException(http_resp=MagicMock(status=400, data="{}"))

    def test_api_exception_code_409(self):
        with IgnoreIfExists():
            raise ApiException(http_resp=MagicMock(status=409, data="{}"))
