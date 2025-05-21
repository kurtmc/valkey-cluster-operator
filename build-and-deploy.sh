#!/bin/bash

NEW_TAG="$(dagger call publish-docker)"
kubectl config use-context halter-sandbox && aws-vault exec halter-sandbox -- make deploy IMG="quay.io/kurtmcalpine/valkey-cluster-operator:${NEW_TAG}"
