# Copyright (c) 2018 Ultimaker B.V.
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging

from mongoOperator.MongoOperator import MongoOperator


if __name__ == '__main__':
    logging.info("Staring Mongo Operator...")
    operator = MongoOperator()
    operator.run()
