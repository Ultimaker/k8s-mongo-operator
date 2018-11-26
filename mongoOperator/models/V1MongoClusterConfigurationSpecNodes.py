# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from mongoOperator.models.BaseModel import BaseModel
from mongoOperator.models.fields import StringField


class V1MongoClusterConfigurationSpecNodes(BaseModel):
    """
    Model for the `spec.nodes` field of the V1MongoClusterConfiguration.
    """
    key = StringField(required=True)
    node_pool = StringField(required=True)
