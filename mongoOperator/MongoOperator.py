# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import threading
from time import sleep

from mongoOperator.managers.EventManager import EventManager
from mongoOperator.managers.PeriodicalCheckManager import PeriodicalCheckManager


class MongoOperator:
    """
    The Mongo operator manages MongoDB replica sets and backups in a Kubernetes cluster.
    """

    def __init__(self) -> None:
        self._shutting_down = threading.Event()
        self._manager_threads = []
        
        # Create the periodic replica set check in a separate thread.
        self._manager_threads.append(threading.Thread(
            name = "PeriodicCheck",
            target = self._startPeriodicalCheck,
            args = (self._shutting_down, 10)
        ))
        
        # Create the Kubernetes event listener in a separate thread.
        self._manager_threads.append(threading.Thread(
            name = "EventListener",
            target = self._startEventListener,
            args = (self._shutting_down, 10)
        ))

    def run(self):
        try:
            while True:
                # Run the short-lived manager threads every 5 seconds but only allow a single instance per type.
                for thread in self._manager_threads:
                    if not thread.ident:
                        thread.start()
                sleep(5)
        except KeyboardInterrupt:
            logging.info("Application interrupted, stopping threads gracefully...")
            self._shutting_down.set()
            for thread in self._manager_threads:
                thread.join()
    
    @staticmethod
    def _startPeriodicalCheck(shutting_down_event: "threading.Event", sleep_seconds: int) -> None:
        manager = PeriodicalCheckManager(shutting_down_event, sleep_seconds)
        manager.run()
    
    @staticmethod
    def _startEventListener(shutting_down_event: "threading.Event", sleep_seconds: int) -> None:
        manager = EventManager(shutting_down_event, sleep_seconds)
        manager.run()
