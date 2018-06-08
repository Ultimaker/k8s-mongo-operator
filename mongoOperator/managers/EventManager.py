# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from enum import Enum
from typing import Dict

from kubernetes.client.rest import ApiException
from kubernetes.watch import Watch
from urllib3.exceptions import ReadTimeoutError

from mongoOperator.managers.Manager import Manager
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService


class EventTypes(Enum):
    """Allowed Kubernetes event types."""
    ADDED = "ADDED"
    MODIFIED = "MODIFIED"
    DELETED = "DELETED"


class EventManager(Manager):
    """
    Manager that processes Kubernetes events.
    """

    kubernetes_service = KubernetesService()

    event_watcher = Watch()

    CONNECT_TIMEOUT = 5.0
    READ_TIMEOUT = 3.0
    REQUEST_TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

    def execute(self) -> None:
        """Execute the manager logic."""
        try:
            next_event = next(self.event_watcher.stream(self.kubernetes_service.listMongoObjects,
                                                        _request_timeout = self.REQUEST_TIMEOUT), None)
        except ReadTimeoutError:
            return
        logging.debug("Received event %s", next_event)

        if next_event and next_event.get("object"):
            self._processEvent(next_event)
            # Reproduces fix found in https://github.com/kubernetes-client/python-base/pull/64/files
            self.event_watcher.resource_version = next_event['object'].get('metadata', {}).get('resourceVersion')

    @classmethod
    def beforeShuttingDown(cls) -> None:
        """Stop the event watcher before closing the thread."""
        cls.event_watcher.stop()

    def _processEvent(self, event: Dict[str, any]) -> None:
        """
        Process the Kubernetes event.
        :param event: The Kubernetes event.
        """
        logging.info("Processing event %s", event)

        if "type" not in event or "object" not in event:
            # This event is not valid for us.
            logging.warning("Received malformed event: {}".format(event))
            return

        if event["type"] not in EventTypes.__members__:
            # This event is not any of the allowed types.
            logging.warning("Received unknown event type: {}".format(event["type"]))
            return

        cluster_object = V1MongoClusterConfiguration(**event["object"])

        # Map event types to handler methods.
        event_type_to_action_map = {
            EventTypes.ADDED.name: self._add,
            EventTypes.MODIFIED.name: self._update,
            EventTypes.DELETED.name: self._delete
        }
        
        # Call the needed handler method.
        try:
            event_type_to_action_map[event["type"]](cluster_object)
        except ApiException:
            logging.error("API error with %s object %s.", event["type"], event["object"])
            raise

    def _add(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """
        Handler method for adding a new managed Mongo replica set.
        """
        self.kubernetes_service.createOperatorAdminSecret(cluster_object)
        self.kubernetes_service.createService(cluster_object)
        self.kubernetes_service.createStatefulSet(cluster_object)

    def _update(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """
        Handler method for updating a managed Mongo replica set.
        """
        # operator admin secret is randomly generated so it cannot be updated
        self.kubernetes_service.updateService(cluster_object)
        self.kubernetes_service.updateStatefulSet(cluster_object)

    def _delete(self, cluster_object: V1MongoClusterConfiguration) -> None:
        """
        Handler method for deleting a managed Mongo replica set.
        """
        name = cluster_object.metadata.name
        namespace = cluster_object.metadata.namespace
        self.kubernetes_service.deleteStatefulSet(name, namespace)
        self.kubernetes_service.deleteService(name, namespace)
        self.kubernetes_service.deleteOperatorAdminSecret(name, namespace)
