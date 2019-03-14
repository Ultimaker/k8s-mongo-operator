# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from mongoOperator.models.BaseModel import BaseModel
from mongoOperator.models.fields import StringField, MongoReplicaCountField


class V1MongoClusterConfigurationSpecMongoDB(BaseModel):
    """
    Model for the `spec.mongodb` field of the V1MongoClusterConfiguration.
    """

    # The name of the deployment.
    mongo_name = StringField(required=False)

    # The name of the volumes that Kubernetes will create and mount. Defaults to mongo-storage.
    storage_name = StringField(required=False)

    # The size of the volumes that Kubernetes will create and mount. Defaults to 30Gi.
    storage_size = StringField(required=False)

    # The path on which the volumes should be mounted. Defaults to /data/db.
    storage_data_path = StringField(required=False)

    # The Kubernetes storage class to use in Kubernetes. Defaults to None.
    storage_class_name = StringField(required=False)

    # Kubernetes CPU limit of each Mongo container. Defaults to 1 (vCPU).
    cpu_limit = StringField(required=False)

    # Kubernetes CPU request of each Mongo container. Defaults to 0.5 (vCPU).
    cpu_request = StringField(required=False)

    # Kubernetes memory limit of each Mongo container. Defaults to 2Gi.
    memory_limit = StringField(required=False)

    # Kubernetes memory request of each Mongo container. Defaults to 1Gi.
    memory_request = StringField(required=False)

    # Amount of Mongo container replicas. Defaults to 3.
    replicas = MongoReplicaCountField(required=True)

    # The wired tiger cache size. Defaults to 0.25.
    # Should be half of the memory limit minus 1 GB.
    # See https://docs.mongodb.com/manual/administration/production-notes/#allocate-sufficient-ram-and-cpu for details.
    wired_tiger_cache_size = StringField(required=False)
