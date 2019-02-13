#!/usr/bin/env bash
set -e
set -o pipefail

# set the environment of the minikube docker
eval $(minikube docker-env)

readonly NAMESPACE="mongo-operator-cluster"
readonly KUBECTL="kubectl --namespace=${NAMESPACE}"

# build the docker image
docker build --tag ultimaker/k8s-mongo-operator:local .

# print out the Kubernetes client and server versions
${KUBECTL} version || true


if ! kubectl get namespace ${NAMESPACE}; then
    kubectl create namespace ${NAMESPACE}
fi

# remove the deployment, if needed, and apply the new one
${KUBECTL} delete deployment mongo-operator 2>/dev/null || true
${KUBECTL} apply --filename=kubernetes/operators/mongo-operator/service-account.yaml || true
${KUBECTL} apply --filename=kubernetes/operators/mongo-operator/cluster-role.yaml || true
${KUBECTL} apply --filename=kubernetes/operators/mongo-operator/cluster-role-binding.yaml || true
${KUBECTL} apply --filename=kubernetes/operators/mongo-operator/deployment.yaml || true

# show some details about the deployment
${KUBECTL} describe deploy mongo-operator

# create a secret with the google account credentials
${KUBECTL} create secret generic storage-serviceaccount --from-file=json=google_credentials.json || true

# wait for the pod to startup to retrieve its name
sleep 10
POD_NAME=$(${KUBECTL} get pods | grep -e "mongo-operator.*Running" | cut --fields=1 --delimiter=" ")
if [ -z $POD_NAME ]; then
    echo "The operator pod is not running!"
    ${KUBECTL} get pods
    exit 1
fi

# apply the example file
${KUBECTL} apply --filename=examples/mongo-3-replicas-from-backup.yaml

# show the pod logs
${KUBECTL} logs ${POD_NAME} --follow
