# k8s-mongo-operator
MongoDB Operator for Kubernetes.

## Features
The following feature are currently available in this operator:

* Create, update or delete MongoDB replica sets.
* Automatically initialize the replica set configuration in the master node.
* Schedule backups to Google Cloud Storage using a Google service account and `mongodump`.

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

```bash
./buld-and-deploy-local.sh
```

You will also see the operator logs streamed to your console.

## Contributing
Please make a GitHub issue or pull request to help us build this operator.

## Maintainance
The repo is currently maintained by Ultimaker. Contact us via the GitHub issues for questions or suggestions.
