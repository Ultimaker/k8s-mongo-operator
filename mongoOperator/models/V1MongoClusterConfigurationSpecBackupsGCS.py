# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-

from kubernetes.client import V1EnvVarSource

from mongoOperator.models.BaseModel import BaseModel
from mongoOperator.models.fields import StringField, EmbeddedField


class V1MongoClusterConfigurationSpecBackupsGCS(BaseModel):
    """
    Model for the `spec.backups.gcs` field of the V1MongoClusterConfiguration.
    """
    bucket = StringField(required=True)
    prefix = StringField(required=False)
    service_account = EmbeddedField(V1EnvVarSource, required=True)
