# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-

from mongoOperator.models.BaseModel import BaseModel
from mongoOperator.models.V1MongoClusterConfigurationSpecBackups import V1MongoClusterConfigurationSpecBackups
from mongoOperator.models.V1MongoClusterConfigurationSpecMongoDB import V1MongoClusterConfigurationSpecMongoDB
from mongoOperator.models.fields import EmbeddedField


class V1MongoClusterConfigurationSpec(BaseModel):
    """
    Model for the `spec` field of the V1MongoClusterConfiguration.
    """
    backups = EmbeddedField(V1MongoClusterConfigurationSpecBackups, required=True)
    mongodb = EmbeddedField(V1MongoClusterConfigurationSpecMongoDB, required=True)
