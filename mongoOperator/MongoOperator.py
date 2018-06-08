# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from time import sleep

from mongoOperator.helpers.ClusterChecker import ClusterChecker


class MongoOperator:
    """
    The Mongo operator manages MongoDB replica sets and backups in a Kubernetes cluster.
    """

    def __init__(self, sleep_per_run: float = 5.0) -> None:
        """
        :param sleep_per_run: How many seconds we should sleep after each run.
        """
        self._sleep_per_run = sleep_per_run

    def run_forever(self):
        checker = ClusterChecker()
        try:
            for _ in range(100):  # TODO: return this to: while True:
                logging.info("**** Running Cluster Check ****")
                try:
                    checker.checkExistingClusters()
                    checker.collectGarbage()
                    # TODO: checker.streamEvents()
                except Exception as e:
                    logging.exception(e)

                logging.info("Waiting %s seconds", self._sleep_per_run)
                sleep(self._sleep_per_run)
        except KeyboardInterrupt:
            logging.info("Application interrupted...")
        logging.info("Done running operator")
