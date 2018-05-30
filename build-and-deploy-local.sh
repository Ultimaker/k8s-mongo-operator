#!/usr/bin/env bash
docker build --tag ultimaker/mongo-operator:local .
kubectl delete deployment mongo-operator
kubectl apply --filename=kubernetes/operators/mongo-operator/deployment.yaml
