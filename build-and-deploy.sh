#!/bin/bash

NEW_TAG=$(curl -s https://quay.io/v2/kurtmcalpine/valkey-cluster-operator/tags/list | jq -r '.tags[] | select(startswith("v"))' | sed '/-/!{s/$/_/}' | sort -V | sed 's/_$//' | tail -n1 | awk -F. -v OFS=. '{$NF += 1 ; print}')
#make docker-buildx docker-push IMG="quay.io/kurtmcalpine/valkey-cluster-operator:${NEW_TAG}" PLATFORMS="linux/arm64,linux/amd64"
make docker-buildx docker-push IMG="quay.io/kurtmcalpine/valkey-cluster-operator:${NEW_TAG}" PLATFORMS="linux/amd64"
kubectl config use-context halter-sandbox && aws-vault exec halter-sandbox -- make deploy IMG="quay.io/kurtmcalpine/valkey-cluster-operator:${NEW_TAG}"
