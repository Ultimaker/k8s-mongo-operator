# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from abc import ABC

import threading
from time import sleep


class Manager(ABC):
    """
    Base class for threaded managers that execute code periodically.
    """
    
    def __init__(self, shutting_down_event: "threading.Event", sleep_seconds: float) -> None:
        """
        :param shutting_down_event: A threading event that will stop executing when done.
        :param sleep_seconds: How many seconds the manager should wait between executions.
        """
        self._shutting_down_event = shutting_down_event
        self._sleep_seconds = sleep_seconds
    
    def run(self) -> None:
        """
        Run the manager execution code.
        """
        while not self._shutting_down_event.isSet():
            try:
                logging.debug("Executing manager {}...".format(self.__class__.__name__))
                self._execute()
            except Exception as exception:
                logging.error("An exception occurred in a manager: {}".format(exception))
            finally:
                sleep(self._sleep_seconds)
        else:
            logging.info("Thread shutting down...")
            self._beforeShuttingDown()

    def _execute(self) -> None:
        """
        Runs the manager once. Must be implemented in subclasses.
        """
        raise NotImplementedError

    def _beforeShuttingDown(self) -> None:
        """
        Runs a cleanup when the manager is going to shut down.
        """
        raise NotImplementedError
