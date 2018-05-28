# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import threading
from time import sleep


class Manager:
    """
    Base class for threaded managers that execute code periodically.
    """
    
    def __init__(self, shutting_down_event: "threading.Event", sleep_seconds: int) -> None:
        logging.info("Starting manager {}".format(__name__))
        self._shutting_down_event = shutting_down_event
        self._sleep_seconds = sleep_seconds
    
    def run(self) -> None:
        """
        Run the manager execution code.
        """
        while not self._shutting_down_event.isSet():
            try:
                logging.debug("Executing manager...")
                self._execute()
            except Exception as exception:
                logging.error("An exception occurred in a manager: {}".format(exception))
            finally:
                sleep(self._sleep_seconds)
        else:
            logging.info("Thread shutting down...")
            self._beforeShuttingDown()

    def _execute(self) -> None:
        raise NotImplementedError

    def _beforeShuttingDown(self) -> None:
        raise NotImplementedError
