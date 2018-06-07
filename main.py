# Copyright (c) 2018 Ultimaker B.V.
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os

from mongoOperator.MongoOperator import MongoOperator


if __name__ == '__main__':
    logging.basicConfig(level=os.getenv("LOGGING_LEVEL", "DEBUG"))
    logging.info("Starting Mongo Operator...")
    operator = MongoOperator()
    operator.run()
