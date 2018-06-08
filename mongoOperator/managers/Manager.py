# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from abc import ABC, abstractmethod


class Manager(ABC):
    """
    Base class for threaded managers that execute code periodically.
    """
    
    def __init__(self) -> None:
        self.name = self.__class__.__name__
    
    def run(self) -> None:
        """
        Run the manager execution code.
        """
        try:
            logging.debug("Executing manager {}...".format(self.name))
            self.execute()
        except Exception as exception:
            logging.exception("An exception occurred in manager %s: %s", self.name, exception)

    @abstractmethod
    def execute(self) -> None:
        """ Runs the manager once. Must be implemented in subclasses. """

    @classmethod
    def beforeShuttingDown(cls) -> None:
        """ Runs a cleanup when the manager is going to shut down. """
