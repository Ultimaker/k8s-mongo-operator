# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import logging

from types import TracebackType
from typing import Type, Optional

from contextlib import AbstractContextManager

from kubernetes.client.rest import ApiException


class IgnoreIfExists(AbstractContextManager):
    """
    Context manager to be used with the with-statement, that ignores any ApiException with code 409
    (i.e. records that already exist in Kubernetes).

    Usage:
        with IgnoreIfExists():
            api_call_that_may_raise()
    """
    def __exit__(self, exc_type: Optional[Type[Exception]], exc_value: Optional[Exception],
                 traceback: Optional[TracebackType]) -> bool:
        """
        :param exc_type: The exception type, if an exception occurred, or None otherwise.
        :param exc_value: The exception, if an exception occurred, or None otherwise.
        :param traceback: The traceback, if an exception occurred, or None otherwise.
        :return: True if the exception should be ignored, False otherwise.
        """
        if exc_type and isinstance(exc_value, ApiException) and exc_value.status == 409:
            error_dict = json.loads(exc_value.body)
            logging.info("HTTP {code} {status}: {message}.", **error_dict)
            return True
