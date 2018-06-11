# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-

from mongoOperator.models.BaseModel import BaseModel
from mongoOperator.models.V1MongoClusterConfigurationSpecBackupsGCS import V1MongoClusterConfigurationSpecBackupsGCS
from mongoOperator.models.fields import EmbeddedField, StringField


class V1MongoClusterConfigurationSpecBackups(BaseModel):
    """
    Model for the `spec.backups` field of the V1MongoClusterConfiguration.
    """
    gcs = EmbeddedField(V1MongoClusterConfigurationSpecBackupsGCS, required=True)
    cron = StringField(required=False)

