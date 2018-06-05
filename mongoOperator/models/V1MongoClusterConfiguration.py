# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-

from kubernetes.client import V1ObjectMeta

from mongoOperator.models.BaseModel import BaseModel
from mongoOperator.models.V1MongoClusterConfigurationSpec import V1MongoClusterConfigurationSpec
from mongoOperator.models.fields import EmbeddedField, StringField


class V1MongoClusterConfiguration(BaseModel):
    """
    Model that contains the Mongo cluster configuration. See `examples/mongo.yaml` for an example.
    """

    api_version = StringField()
    kind = StringField()
    metadata = EmbeddedField(V1ObjectMeta)
    spec = EmbeddedField(V1MongoClusterConfigurationSpec)
