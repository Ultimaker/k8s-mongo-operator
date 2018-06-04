# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2018 Ultimaker
# !/usr/bin/env python
# -*- coding: utf-8 -*-
from unittest import TestCase
from unittest.mock import patch, call, MagicMock

from kubernetes.client import Configuration, V1Secret, V1ObjectMeta, V1Service, \
    V1ServiceSpec, V1ServicePort, V1DeleteOptions, V1beta1StatefulSet, V1beta1StatefulSetSpec, V1PodSpec, V1Container, \
    V1EnvVar, V1EnvVarSource, V1ObjectFieldSelector, V1ContainerPort, V1VolumeMount, V1ResourceRequirements, \
    V1PersistentVolumeClaim, V1PersistentVolumeClaimSpec, V1PodTemplateSpec, V1beta1CustomResourceDefinitionList
from kubernetes.client.rest import ApiException

from mongoOperator.helpers.KubernetesResources import KubernetesResources
from mongoOperator.models.V1MongoClusterConfiguration import V1MongoClusterConfiguration
from mongoOperator.services.KubernetesService import KubernetesService
from tests.test_utils import getExampleClusterDefinition, dict_eq


@patch("mongoOperator.services.KubernetesService.client")
class TestKubernetesService(TestCase):
    maxDiff = 10000

    def setUp(self):
        super().setUp()
        self.cluster_dict = getExampleClusterDefinition()
        self.cluster_object = V1MongoClusterConfiguration(**self.cluster_dict)
        self.name = self.cluster_object.metadata.name
        self.namespace = self.cluster_object.metadata.namespace

        self.stateful_set = V1beta1StatefulSet(
            metadata=self._createMeta(self.name),
            spec=V1beta1StatefulSetSpec(
                replicas=3,
                service_name=self.name,
                template=V1PodTemplateSpec(
                    metadata = V1ObjectMeta(labels=KubernetesResources.createDefaultLabels(self.name)),
                    spec=V1PodSpec(containers=[V1Container(
                        name="mongodb",
                        env=[V1EnvVar(
                            name="POD_IP",
                            value_from=V1EnvVarSource(
                                field_ref=V1ObjectFieldSelector(api_version = "v1", field_path = "status.podIP")
                            )
                        )],
                        command=["mongod", "--replSet", self.name, "--bind_ip", "0.0.0.0", "--smallfiles", "--noprealloc"],
                        image="mongo:3.6.4",
                        ports=V1ContainerPort(name="mongodb", container_port=27017, protocol="TCP"),
                        volume_mounts=[V1VolumeMount(name="mongo-storage", read_only=False, mount_path="/data/db")],
                        resources=V1ResourceRequirements(
                            limits={"cpu": "100m", "memory": "64Mi"},
                            requests={"cpu": "100m", "memory": "64Mi"}
                        )
                    )])
                ),
                volume_claim_templates=[V1PersistentVolumeClaim(
                    metadata=V1ObjectMeta(name="mongo-storage"),
                    spec=V1PersistentVolumeClaimSpec(
                        access_modes=["ReadWriteOnce"],
                        resources=V1ResourceRequirements(requests={"storage": "30Gi"})
                    )
                )],
            ),
        )

    def _createMeta(self, name: str) -> V1ObjectMeta:
        return V1ObjectMeta(
            labels=KubernetesResources.createDefaultLabels(name),
            name=name,
            namespace=self.namespace,
        )

    def test___init__(self, client_mock):
        KubernetesService()
        config = Configuration()
        config.debug = False
        expected = [
            call.ApiClient(config),
            call.CoreV1Api(client_mock.ApiClient.return_value),
            call.CustomObjectsApi(client_mock.ApiClient.return_value),
            call.ApiextensionsV1beta1Api(client_mock.ApiClient.return_value),
            call.AppsV1beta1Api(client_mock.ApiClient.return_value),
        ]

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
            "metadata": {"name": "mongos.operators.ultimaker.com"},
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
        item.spec.names.plural = "mongos"
        client_mock.ApiextensionsV1beta1Api.return_value.list_custom_resource_definition.return_value.items = [item]

        self.assertIsNone(service.createMongoObjectDefinition())
        expected = [call.ApiextensionsV1beta1Api().list_custom_resource_definition()]
        self.assertEqual(expected, client_mock.mock_calls)

    def test_listMongoObjects(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        item = MagicMock()
        item.spec.names.plural = "mongos"
        client_mock.ApiextensionsV1beta1Api.return_value.list_custom_resource_definition.return_value.items = [item]

        result = service.listMongoObjects(param="value")
        expected_calls = [
            call.ApiextensionsV1beta1Api().list_custom_resource_definition(),
            call.CustomObjectsApi().list_cluster_custom_object('operators.ultimaker.com', 'v1', "mongos", param='value')
        ]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.CustomObjectsApi().list_cluster_custom_object.return_value, result)

    def test_getMongoObject(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        result = service.getMongoObject(self.name, self.namespace)
        expected_calls = [call.CustomObjectsApi().get_namespaced_custom_object(
            'operators.ultimaker.com', 'v1', self.namespace, 'mongos', self.name
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

    @patch("mongoOperator.helpers.KubernetesResources.uuid.uuid4", lambda: MagicMock(hex = "random-password"))
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
        expected_calls = [call.CoreV1Api().create_namespaced_secret(self.namespace, expected_body)]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertIsNone(result)

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
                selector={'heritage': 'mongos', 'name': self.name, 'operated-by': 'operators.ultimaker.com'},
            )
        )
        expected_calls = [call.CoreV1Api().create_namespaced_service(self.namespace, expected_body)]

        result = service.createService(self.cluster_object)
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(V1Service(kind="unit"), result)

    def test_updateService(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        expected_body = V1Service(
            metadata=self._createMeta(self.name),
            spec = V1ServiceSpec(
                cluster_ip="None",
                ports=[V1ServicePort(name='mongod', port=27017, protocol='TCP')],
                selector={'heritage': 'mongos', 'name': self.name, 'operated-by': 'operators.ultimaker.com'},
            )
        )
        result = service.updateService(self.cluster_object)
        expected_calls = [call.CoreV1Api().patch_namespaced_service(self.name, self.namespace, expected_body)]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.CoreV1Api().patch_namespaced_service.return_value, result)

    def test_deleteService(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        result = service.deleteService(self.name, self.namespace)
        expected_calls = [call.CoreV1Api().delete_namespaced_service(self.name, self.namespace, V1DeleteOptions())]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.CoreV1Api().delete_namespaced_service.return_value, result)

    def test_getStatefulSet(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        result = service.getStatefulSet(self.name, self.namespace)
        expected_calls = [call.AppsV1beta1Api().read_namespaced_stateful_set(self.name, self.namespace)]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.AppsV1beta1Api().read_namespaced_stateful_set.return_value, result)

    def test_createStatefulSet(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        expected_calls = [call.AppsV1beta1Api().create_namespaced_stateful_set(self.namespace, self.stateful_set)]

        result = service.createStatefulSet(self.cluster_object)
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.AppsV1beta1Api().create_namespaced_stateful_set.return_value, result)

    def test_updateStatefulSet(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()

        result = service.updateStatefulSet(self.cluster_object)
        expected_calls = [
            call.AppsV1beta1Api().patch_namespaced_stateful_set(self.name, self.namespace, self.stateful_set)
        ]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.AppsV1beta1Api().patch_namespaced_stateful_set.return_value, result)

    def test_deleteStatefulSet(self, client_mock):
        service = KubernetesService()
        client_mock.reset_mock()
        result = service.deleteStatefulSet(self.name, self.namespace)
        expected_calls = [
            call.AppsV1beta1Api().delete_namespaced_stateful_set(self.name, self.namespace, V1DeleteOptions())
        ]
        self.assertEqual(expected_calls, client_mock.mock_calls)
        self.assertEqual(client_mock.AppsV1beta1Api().delete_namespaced_stateful_set.return_value, result)
