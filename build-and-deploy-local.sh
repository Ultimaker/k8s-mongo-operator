#!/usr/bin/env bash
docker build -t ultimaker/mongo-operator:local .
kubectl delete deployment mongo-operator
kubectl apply -f kubernetes/operators/mongo-operator/deployment.yaml