# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase

from mongoOperator.services.KubernetesService import KubernetesService
from mongoOperator.services.MongoService import MongoService
from tests.test_utils import getExampleClusterDefinition


class TestMongoService(TestCase):  #TODO

    def setUp(self):
        super().setUp()
        self.kubernetes_service = KubernetesService()
        self.service = MongoService(self.kubernetes_service)
        self.cluster_object = getExampleClusterDefinition()
