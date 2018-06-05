# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import yaml


def getExampleClusterDefinition() -> dict:
    with open("./examples/mongo.yaml") as f:
        return yaml.load(f)


def dict_eq(one, other):
    # [(k, getattr(self, k), getattr(other, k)) for k in self.__dict__ if getattr(self, k) != getattr(other, k)]
    return other and one.__dict__ == other.__dict__
