# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-

from kubernetes.client import V1SecretKeySelector

from mongoOperator.models.BaseModel import BaseModel
from mongoOperator.models.fields import EmbeddedField


class V1ServiceAccountRef(BaseModel):
    """
    Model for the `spec.backups.gcs.service_account` field of the V1MongoClusterConfiguration.
    """
    secret_key_ref = EmbeddedField(V1SecretKeySelector, required=True)
