#!/bin/bash

# Set environment variables
export IMAGE_TAG="latest"  # Or use a specific tag
export NAMESPACE="my-namespace"

# Create the namespace if it doesn't exist
kubectl create namespace "$NAMESPACE" 2>/dev/null || true

# Apply the Kubernetes manifests
kubectl apply -n "$NAMESPACE" -f deployment.yaml
kubectl apply -n "$NAMESPACE" -f service.yaml

echo "Application deployed to Kubernetes namespace '$NAMESPACE'"
