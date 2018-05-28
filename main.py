# Copyright (c) 2018 Ultimaker B.V.
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from mongoOperator.MongoOperator import MongoOperator


if __name__ == '__main__':
    operator = MongoOperator()
    operator.run()
