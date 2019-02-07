#!/usr/bin/env bash

POD_NAME=$(kubectl get pods | grep -e "mongo-operator.*Running" | cut --fields=1 --delimiter=" ")
if [ -z $POD_NAME ]; then
    echo "The operator pod is not running!"
    kubectl get pods
    exit 1
fi

# apply the example file
kubectl apply --filename=examples/mongo-3-replicas-from-restore.yaml

# show the pod logs
kubectl logs ${POD_NAME} --follow
