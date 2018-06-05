# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
from enum import Enum

from kubernetes.client.rest import ApiException
from kubernetes.watch import Watch

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
    
    event_watcher = Watch()
    kubernetes_service = KubernetesService()

    def execute(self) -> None:
        """Execute the manager logic."""
        for event in self.event_watcher.stream(self.kubernetes_service.listMongoObjects,
                                               _request_timeout = self._sleep_seconds):
            self._processEvent(event)

    @classmethod
    def beforeShuttingDown(cls) -> None:
        """Stop the event watcher before closing the thread."""
        cls.event_watcher.stop()

    def _processEvent(self, event) -> None:
        """
        Process the Kubernetes event.
        :param event: The Kubernetes event.
        """
        if "type" not in event or "object" not in event:
            # This event is not valid for us.
            logging.warning("Received malformed event: {}".format(event))
            return
        
        if event["type"] not in EventTypes.__members__:
            # This event is not any of the allowed types.
            logging.warning("Received unknown event type: {}".format(event["type"]))
            return

        # Map event types to handler methods.
        event_type_to_action_map = {
            EventTypes.ADDED.name: self._add,
            EventTypes.MODIFIED.name: self._update,
            EventTypes.DELETED.name: self._delete
        }
        
        # Call the needed handler method.
        try:
            cluster_object = V1MongoClusterConfiguration(**event["object"])
            print(cluster_object)
            event_type_to_action_map[event["type"]](cluster_object)
        except ApiException as error:
            logging.exception("API error with %s object %s: %s", event["type"], event["object"], error)
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
