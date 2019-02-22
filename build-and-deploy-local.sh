#!/usr/bin/env bash
set -eo pipefail

EXAMPLE_FILE=${1:-examples/mongo-3-replicas.yaml}

if ! [[ -e "google-credentials.json" ]]; then
    echo "google-credentials.json file is missing, aborting."
    exit -1
fi

# set the environment of the minikube docker
eval $(minikube docker-env)

readonly NAMESPACE="mongo-operator-cluster"
readonly KUBECTL="kubectl --namespace=${NAMESPACE}"

# build the docker image
docker build --tag ultimaker/k8s-mongo-operator:local .

# print out the Kubernetes client and server versions
${KUBECTL} version

if ! kubectl get namespace ${NAMESPACE}; then
    kubectl create namespace ${NAMESPACE}
fi

# remove the deployment, if needed, and apply the new one
${KUBECTL} delete deployment mongo-operator 1>/dev/null
${KUBECTL} apply --filename=kubernetes/operators/mongo-operator/service-account.yaml
${KUBECTL} apply --filename=kubernetes/operators/mongo-operator/cluster-role.yaml
${KUBECTL} apply --filename=kubernetes/operators/mongo-operator/cluster-role-binding.yaml
${KUBECTL} apply --filename=kubernetes/operators/mongo-operator/deployment.yaml

# show some details about the deployment
${KUBECTL} describe deploy mongo-operator

# create a secret with the google account credentials
if ${KUBECTL} get secret storage-serviceaccount 1>/dev/null; then
    ${KUBECTL} delete secret storage-serviceaccount
fi
${KUBECTL} create secret generic storage-serviceaccount --from-file=json=google-credentials.json

# wait for the pod to startup to retrieve its name
sleep 10
POD_NAME=$(${KUBECTL} get pod -l app=mongo-operator -o jsonpath="{.items[0].metadata.name}")
if [ -z $POD_NAME ]; then
    echo "The operator pod is not running!"
    ${KUBECTL} get pods
    exit 1
fi

# apply the example file
${KUBECTL} apply --filename=${EXAMPLE_FILE}

# show the pod logs
${KUBECTL} logs ${POD_NAME} --follow
