# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase
from unittest.mock import patch, call, MagicMock

from kubernetes.client import V1beta1CustomResourceDefinitionList, Configuration, V1Secret, V1ObjectMeta, V1Service, \
    V1ServiceSpec, V1ServicePort, V1DeleteOptions
from kubernetes.client.rest import ApiException

from mongoOperator.helpers.KubernetesResources import KubernetesResources
from mongoOperator.services.KubernetesService import KubernetesService
from tests.test_utils import getExampleClusterDefinition, dict_eq


@patch("mongoOperator.services.KubernetesService.client")
class TestKubernetesService(TestCase):
    maxDiff = None

    def setUp(self):
        super().setUp()
        self.cluster_object = getExampleClusterDefinition()
        self.name = self.cluster_object.metadata.name
        self.namespace = self.cluster_object.metadata.namespace

    def _createMeta(self, name: str) -> V1ObjectMeta:
        return V1ObjectMeta(
            labels=KubernetesResources.createDefaultLabels(name),
            name=name,
            namespace=self.namespace,
        )

    def test___init__(self, client_mock):
        kubernetes_config = Configuration()
        kubernetes_config.host = "http://localhost"
        kubernetes_config.verify_ssl = False
        kubernetes_config.debug = False
        expected = [
            call.ApiClient(configuration=kubernetes_config),
            call.CustomObjectsApi(client_mock.ApiClient.return_value),
            call.CoreV1Api(client_mock.ApiClient.return_value),
            call.ApiextensionsV1beta1Api(client_mock.ApiClient.return_value),
            call.AppsV1beta1Api(client_mock.ApiClient.return_value),
        ]

        KubernetesService()
        with patch("kubernetes.client.configuration.Configuration.__eq__", dict_eq):
            self.assertEqual(expected, client_mock.mock_calls)

    def test_createMongoObjectDefinition(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()
        client_mock.ApiextensionsV1beta1Api.return_value.list_custom_resource_definition.return_value = \
            V1beta1CustomResourceDefinitionList(items=[])

        expected_def = {
            "apiVersion": "apiextensions.k8s.io/v1beta1",
            "kind": "CustomResourceDefinition",
            "metadata": {"name": "mongo.operators.ultimaker.com"},
            "spec": {
                "group": "operators.ultimaker.com", "version": "v1", "scope": "Namespaced",
                "names": {
                    "plural": "mongos",
                    "singular": "mongo",
                    "kind": "Mongo",
                    "shortNames": ["mng"]
                }
            }
        }

        self.assertIsNone(service.createMongoObjectDefinition())
        expected_calls = [
            call.ApiextensionsV1beta1Api().list_custom_resource_definition(),
            call.ApiextensionsV1beta1Api().create_custom_resource_definition(expected_def),
        ]
        self.assertEqual(expected_calls, client_mock.mock_calls)

    def test_createMongoObjectDefinition_existing(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        item = MagicMock()
        item.spec.names.kind = "mongo"
        client_mock.ApiextensionsV1beta1Api.return_value.list_custom_resource_definition.return_value.items = [item]

        self.assertIsNone(service.createMongoObjectDefinition())
        expected = [call.ApiextensionsV1beta1Api().list_custom_resource_definition()]
        self.assertEqual(expected, client_mock.mock_calls)

    def test_listMongoObjects(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        item = MagicMock()
        item.spec.names.kind = "mongo"
        client_mock.ApiextensionsV1beta1Api.return_value.list_custom_resource_definition.return_value.items = [item]

        result = service.listMongoObjects(param="value")
        expected_calls = [
            call.ApiextensionsV1beta1Api().list_custom_resource_definition(),
            call.CustomObjectsApi().list_cluster_custom_object('operators.ultimaker.com', 'v1', "mongo", param='value')
        ]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.CustomObjectsApi().list_cluster_custom_object.return_value, result)

    def test_getMongoObject(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        result = service.getMongoObject(self.name, self.namespace)
        expected_calls = [call.CustomObjectsApi().get_namespaced_custom_object(
            'operators.ultimaker.com', 'v1', self.namespace, 'mongo', self.name
        )]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.CustomObjectsApi().get_namespaced_custom_object.return_value, result)

    def test_listAllServicesWithLabels_default(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        result = service.listAllServicesWithLabels()
        expected_calls = [
            call.CoreV1Api().list_service_for_all_namespaces(label_selector=KubernetesResources.createDefaultLabels())
        ]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.CoreV1Api().list_service_for_all_namespaces.return_value, result)

    def test_listAllServicesWithLabels_custom(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        label_selector = {"operated-by": "me", "heritage": "mongo", "name": "name"}
        result = service.listAllServicesWithLabels(label_selector)
        expected_calls = [call.CoreV1Api().list_service_for_all_namespaces(label_selector=label_selector)]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.CoreV1Api().list_service_for_all_namespaces.return_value, result)

    def test_listAllStatefulSetsWithLabels_default(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        result = service.listAllStatefulSetsWithLabels()
        expected_calls = [call.AppsV1beta1Api().list_stateful_set_for_all_namespaces(
            label_selector=KubernetesResources.createDefaultLabels()
        )]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.AppsV1beta1Api().list_stateful_set_for_all_namespaces.return_value, result)

    def test_listAllStatefulSetsWithLabels_custom(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        label_selector = {"operated-by": "me", "heritage": "mongo", "name": "name"}
        result = service.listAllStatefulSetsWithLabels(label_selector)
        expected_calls = [call.AppsV1beta1Api().list_stateful_set_for_all_namespaces(label_selector=label_selector)]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.AppsV1beta1Api().list_stateful_set_for_all_namespaces.return_value, result)

    def test_listAllSecretsWithLabels_default(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        result = service.listAllSecretsWithLabels()
        expected_calls = [
            call.CoreV1Api().list_secret_for_all_namespaces(label_selector=KubernetesResources.createDefaultLabels())
        ]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.CoreV1Api().list_secret_for_all_namespaces.return_value, result)

    def test_listAllSecretsWithLabels_custom(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        label_selector = {"operated-by": "me", "heritage": "mongo", "name": "name"}
        result = service.listAllSecretsWithLabels(label_selector)
        expected_calls = [
            call.CoreV1Api().list_secret_for_all_namespaces(label_selector=label_selector)
        ]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.CoreV1Api().list_secret_for_all_namespaces.return_value, result)

    @patch("mongoOperator.helpers.KubernetesResources.KubernetesResources.createRandomPassword", lambda: "random-password")
    def test_createOperatorAdminSecret(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        result = service.createOperatorAdminSecret(self.cluster_object)
        expected_body = V1Secret(
            metadata = self._createMeta(self.name + "-admin-credentials"),
            string_data={"password": "random-password", "username": "root"},
        )
        self.assertEqual([call.CoreV1Api().create_namespaced_secret(self.namespace, expected_body)],
                         client_mock.mock_calls)

        self.assertEqual(client_mock.CoreV1Api.return_value.create_namespaced_secret.return_value, result)

    def test_deleteOperatorAdminSecret(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        expected_body = V1DeleteOptions()
        expected_calls = [
            call.CoreV1Api().delete_namespaced_secret(self.name + "-admin-credentials", self.namespace, expected_body)
        ]
        result = service.deleteOperatorAdminSecret(self.name, self.namespace)
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.CoreV1Api.return_value.delete_namespaced_secret.return_value, result)

    def test_getSecret(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        result = service.getSecret(self.name, self.namespace)
        expected_calls = [call.CoreV1Api().read_namespaced_secret(self.name, self.namespace)]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.CoreV1Api().read_namespaced_secret.return_value, result)

    def test_createSecret(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        secret_data = {"user": "unit-test", "password": "secret"}
        expected_body = V1Secret(metadata = self._createMeta("secret-name"), string_data=secret_data)
        result = service.createSecret("secret-name", self.namespace, secret_data)

        self.assertEqual([call.CoreV1Api().create_namespaced_secret(self.namespace, expected_body)],
                         client_mock.mock_calls)
        self.assertEqual(client_mock.CoreV1Api.return_value.create_namespaced_secret.return_value, result)

    def test_createSecret_exists(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()
        client_mock.CoreV1Api.return_value.create_namespaced_secret.side_effect = ApiException(status=409)

        secret_data = {"user": "unit-test", "password": "secret"}
        result = service.createSecret(self.name, self.namespace, secret_data)

        expected_body = V1Secret(metadata = self._createMeta(self.name), string_data=secret_data)
        expected_calls = [
            call.CoreV1Api().create_namespaced_secret(self.namespace, expected_body),
            call.CoreV1Api().read_namespaced_secret(self.name, self.namespace)
        ]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.CoreV1Api.return_value.read_namespaced_secret.return_value, result)

    def test_createSecret_error(self, client_mock):
        service = KubernetesService()
        client_mock.CoreV1Api.return_value.create_namespaced_secret.side_effect = ApiException(status=400)
        with self.assertRaises(ApiException):
            service.createSecret(self.name, self.namespace, secret_data={})

    def test_updateSecret(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        client_mock.CoreV1Api.return_value.read_namespaced_secret.return_value = V1Secret(kind="unit")

        secret_data = {"user": "unit-test", "password": "secret"}
        expected_body = V1Secret(kind="unit", string_data=secret_data)
        expected_calls = [
            call.CoreV1Api().read_namespaced_secret(self.name, self.namespace),
            call.CoreV1Api().patch_namespaced_secret(self.name, self.namespace, expected_body),
        ]

        result = service.updateSecret(self.name, self.namespace, secret_data)
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.CoreV1Api.return_value.patch_namespaced_secret.return_value, result)

    def test_deleteSecret(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        result = service.deleteSecret(self.name, self.namespace)
        expected_calls = [call.CoreV1Api().delete_namespaced_secret(self.name, self.namespace, V1DeleteOptions())]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.CoreV1Api.return_value.delete_namespaced_secret.return_value, result)

    def test_getService(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        result = service.getService(self.name, self.namespace)
        expected_calls = [call.CoreV1Api().read_namespaced_service(self.name, self.namespace)]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.CoreV1Api.return_value.read_namespaced_service.return_value, result)

    def test_createService(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()
        client_mock.CoreV1Api.return_value.create_namespaced_service.return_value = V1Service(kind="unit")

        expected_body = V1Service(
            metadata=self._createMeta(self.name),
            spec = V1ServiceSpec(
                cluster_ip="None",
                ports=[V1ServicePort(name='mongod', port=27017, protocol='TCP')],
                selector={'heritage': 'mongo', 'name': self.name, 'operated-by': 'operators.ultimaker.com'},
            )
        )
        expected_calls = [call.CoreV1Api().create_namespaced_service(self.namespace, expected_body)]

        result = service.createService(self.cluster_object)
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(V1Service(kind="unit"), result)

    # TODO:
    # def test_updateService(self, client_mock):
    #     service = KubernetesService()
    #     client_mock.reset_mock()
    #
    #     result = service.updateService(self.cluster_object)
    #     expected_calls = []
    #     self.assertEqual(expected_calls, client_mock.mock_calls)
    #     self.assertEqual(client_mock, result)

    # TODO:
    # def test_deleteService(self, client_mock):
    #     service = KubernetesService()
    #     client_mock.reset_mock()
    #
    #     result = service.deleteService(self.name, self.namespace)
    #     expected_calls = []
    #     self.assertEqual(expected_calls, client_mock.mock_calls)
    #     self.assertEqual(client_mock, result)

    # TODO:
    # def test_getStatefulSet(self, client_mock):
    #     service = KubernetesService()
    #     client_mock.reset_mock()
    #
    #     result = service.getStatefulSet(self.name, self.namespace)
    #     expected_calls = []
    #     self.assertEqual(expected_calls, client_mock.mock_calls)
    #     self.assertEqual(client_mock, result)

    # TODO:
    # def test_createStatefulSet(self, client_mock):
    #     service = KubernetesService()
    #     client_mock.reset_mock()
    #
    #     result = service.createStatefulSet(self.cluster_object)
    #     expected_calls = []
    #    self.assertEqual(expected_calls, client_mock.mock_calls)
    #     self.assertEqual(client_mock, result)

    # TODO:
    # def test_updateStatefulSet(self, client_mock):
    #     service = KubernetesService()
    #     client_mock.reset_mock()
    #
    #     result = service.updateStatefulSet(self.cluster_object)
    #     expected_calls = []
    #     self.assertEqual(expected_calls, client_mock.mock_calls)
    #     self.assertEqual(client_mock, result)

    # TODO:
    # def test_deleteStatefulSet(self, client_mock):
    #     service = KubernetesService()
    #     client_mock.reset_mock()
    #     result = service.deleteStatefulSet(self.name, self.namespace)
    #     expected_calls = []
    #     self.assertEqual(expected_calls, client_mock.mock_calls)
    #     self.assertEqual(client_mock, result)
