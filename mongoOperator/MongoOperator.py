# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from time import sleep

from mongoOperator.managers.PeriodicalCheckManager import PeriodicalCheckManager


class MongoOperator:
    """
    The Mongo operator manages MongoDB replica sets and backups in a Kubernetes cluster.
    """

    def __init__(self, sleep_per_manager: float = 5.0, sleep_per_run: float = 5.0) -> None:
        """
        :param sleep_per_manager: How many seconds we should sleep after each of the managers runs once.
        :param sleep_per_run: How many seconds we should sleep after all the managers have run.
        """
        self._sleep_per_manager = sleep_per_manager
        self._sleep_per_run = sleep_per_run

        self._managers = [PeriodicalCheckManager()]

    def run_forever(self):
        try:
            for _ in range(10):  # TODO: return this to: while True:
                logging.info("********* Starting to run %s managers *********", len(self._managers))
                for manager in self._managers:
                    logging.info("Running manager %s...", manager.name)
                    manager.run()
                    logging.info("Done running %s. Waiting %s seconds", manager.name, self._sleep_per_run)
                    sleep(self._sleep_per_manager)
                logging.info("Done running all managers. Waiting %s seconds", self._sleep_per_run)
                sleep(self._sleep_per_run)
        except KeyboardInterrupt:
            logging.info("Application interrupted, stopping managers gracefully...")
            for manager in self._managers:
                manager.beforeShuttingDown()
        logging.info("Done running operator")
