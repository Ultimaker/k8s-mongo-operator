# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import os

STRING_TO_BOOL_DICT = {"True", "true", "yes", "1"}


class Settings:
    """
    Class responsible for keeping the application settings.
    """

    # Custom resource (CRD) API config.
    CUSTOM_OBJECT_API_GROUP = "operators.ultimaker.com"
    CUSTOM_OBJECT_API_VERSION = "v1"
    CUSTOM_OBJECT_RESOURCE_PLURAL = "mongos"

    # Kubernetes config.
    KUBERNETES_SERVICE_DEBUG = os.getenv("KUBERNETES_SERVICE_DEBUG") in STRING_TO_BOOL_DICT
