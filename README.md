# k8s-mongo-operator
MongoDB Operator for Kubernetes.

[![Docker Build Status](https://img.shields.io/docker/build/ultimaker/k8s-mongo-operator.svg)](https://hub.docker.com/r/ultimaker/k8s-mongo-operator)

## Features
The following feature are currently available in this operator:

* Create, update or delete MongoDB replica sets.
* Automatically initialize the replica set configuration in the master node.
* Schedule backups to Google Cloud Storage using a Google service account and `mongodump`.

## Limitations
The current version has the limitations that shall be addressed in a later version:

- The watch API from Kubernetes is currently not being used, as we want to remain responsive for creating backups in case no events are received. This means:
  - We use list secret privilege to remove any admin operator secrets that are not used anymore. This is not part of the [best practices](https://kubernetes.io/docs/concepts/configuration/secret/#best-practices).
  - The solution is probably to listen to events with [`asyncio`](https://engineering.bitnami.com/articles/kubernetes-async-watches.html).
- Mongo instances are not using SSL certificates yet.

## Cluster interaction
Please refer to our [simplified diagram](./docs/architecture.png) to get an overview of the operator interactions with your Kubernetes cluster.

## Deployment
To deploy this operator in your own cluster, you'll need to create some configuration files.
An example of these configuration files can be found in [kubernetes/operators/mongo-operator](./kubernetes/operators/mongo-operator)

As you can see there is a service account (mongo-operator-service-account) which has some specific permissions in the cluster.
These permissions are registered in the cluster role and cluster role binding.

Lastly there is a deployment configuration to deploy the actual operator.
Usually you'd use an image value like `ultimaker/k8s-mongo-operator:master`, or a specific version.
All available tags can be found on [Docker Hub](https://hub.docker.com/r/ultimaker/k8s-mongo-operator/).

## Creating a Mongo object
To deploy a new replica set in your cluster using the operator, create a Kubernetes configuration file similar to this:

```yaml
apiVersion: "operators.ultimaker.com/v1"
kind: Mongo
metadata:
  name: mongo-cluster
spec:
  mongodb:
    replicas: 3
  backups:
    cron: "0 * * * *" # hourly
    gcs:
      bucket: "ultimaker-mongo-backups"
      serviceAccount:
        secretKeyRef:
          name: "storage-service-account"
          key: json
```

Then deploy it to the cluster like any other object:

```bash
kubectl apply -f mongo.yaml
```

### Configuration options
The following options are available to use in the `spec` section of the `yaml` configuration file:

| Config key | Default value | Description |
| --- | --- | --- |
| `mongodb.mongo_name` |  | The name of the Mongo deployment. |
| `mongodb.storage_name` | mongo-storage | The name of the persistent volumes that Kubernetes will create and mount. |
| `mongodb.storage_size` | 30Gi | The size of the persistent volumes. |
| `mongodb.storage_data_path` | /data/db | The path on which the persistent volumes are mounted in the Mongo containers. |
| `mongodb.storage_class_name` | - | The name of the storage class to use to create the persistent values. If not passed it will use the Kubernetes cluster default storage class name. |
| `mongodb.cpu_limit` | 1 | The CPU limit of each container. |
| `mongodb.memory_limit` | 2Gi | The memory limit of each container. |
| `mongodb.wired_tiger_cache_size` | 0.25 | The wired tiger cache size. |
| `mongodb.replicas` | No default, this is a required field. | The amount of MongoDB replicas that should be available in the replica set. Must be an uneven positive integer and minimum 3. |

> Please check https://docs.mongodb.com/manual/administration/production-notes/#allocate-sufficient-ram-and-cpu for details about why setting the WiredTiger cache size is important when you change the container memory limit from the default value.

## Testing locally
To run the tests in a local Kubernetes (MiniKube) cluster, we have created a simple test script.

Ensure you have the following tools installed on your system:
- [Docker](https://store.docker.com/search?type=edition&offering=community)
- [MiniKube v0.25.2](https://github.com/kubernetes/minikube/releases/tag/v0.25.2) (please use this version specifically)

Then start a new MiniKube cluster using the following commands:

```bash
minikube start
```

Then you can run our test script to deploy the operator and execute some end-to-end tests.

Note that this script assumes there is a file `google_credentials.json` in this directory that will be uploaded to Kubernetes as the secret for the backups.
You will need to download this file from Google in order to run the script.

```bash
./build-and-deploy-local.sh
```

You will also see the operator logs streamed to your console.

## Contributing
Please make a GitHub issue or pull request to help us build this operator.

## Maintainance
The repo is currently maintained by Ultimaker. Contact us via the GitHub issues for questions or suggestions.
