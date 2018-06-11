# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-

from mongoOperator.models.BaseModel import BaseModel
from mongoOperator.models.fields import StringField, MongoReplicaCountField


class V1MongoClusterConfigurationSpecMongoDB(BaseModel):
    """
    Model for the `spec.mongodb` field of the V1MongoClusterConfiguration.
    """
    cpu_limit = StringField(required=False)

    memory_limit = StringField(required=False)

    replicas = MongoReplicaCountField(required=True)
