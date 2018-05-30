# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import yaml
from kubernetes.client import V1ObjectMeta, V1beta1CustomResourceDefinition


def getExampleClusterDefinition():
    with open("./examples/mongo.yaml") as f:
        cluster_dict = yaml.load(f)
    cluster_dict["api_version"] = cluster_dict.pop("apiVersion")
    cluster_dict["metadata"] = V1ObjectMeta(**cluster_dict["metadata"])
    return V1beta1CustomResourceDefinition(**cluster_dict)
