# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase

from mongoOperator.services.MongoService import MongoService
from tests.test_utils import getExampleClusterDefinition


class TestMongoService(TestCase):

    def setUp(self):
        super().setUp()
        self.service = MongoService()
        self.cluster_object = getExampleClusterDefinition()

    def test_getMemberHostname(self):
        self.assertIsNone(self.service.getMemberHostname())

    def test_checkReplicaSetNeedsSetup(self):
        self.assertIsNone(self.service.checkReplicaSetNeedsSetup(self.cluster_object))

    def test_initializeReplicaSet(self):
        self.assertIsNone(self.service.initializeReplicaSet())

    def test_createUsers(self):
        self.assertIsNone(self.service.createUsers())
