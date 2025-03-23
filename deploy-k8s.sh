#!/bin/bash

# Step 1: Apply Kubernetes Manifests
echo "Applying Kubernetes manifests..."
kubectl apply -f k8s/rabbitmq-deployment.yaml
kubectl apply -f k8s/data-loader-deployment.yaml
kubectl apply -f k8s/data-processor-deployment.yaml
kubectl apply -f k8s/analysis-deployment.yaml
kubectl apply -f k8s/visualization-deployment.yaml

# Step 2: Verify Deployment
echo "Waiting for pods to be in Running state..."
sleep 10  # Wait for pods to initialize

# Check pod status
kubectl get pods

# Check service status
kubectl get services

# Wait for all pods to be in Running state
echo "Waiting for all pods to be ready..."
kubectl wait --for=condition=Ready pods --all --timeout=300s

# Step 3: Access RabbitMQ Management UI
echo "Forwarding port for RabbitMQ Management UI..."
kubectl port-forward service/rabbitmq 15672:15672 &
RABBITMQ_PID=$!  # Save the process ID for later

echo "RabbitMQ Management UI is available at:"
echo "http://localhost:15672"
echo "Username: atlas"
echo "Password: atlas"

# Step 4: Access Visualization Output
echo "Forwarding port for Visualization service..."
kubectl port-forward service/visualization 8080:80 &
VISUALIZATION_PID=$!  # Save the process ID for later

echo "Visualization output is available at:"
echo "http://localhost:8080"

# Keep the script running to keep port-forwarding active
echo "Press Ctrl+C to stop port-forwarding and exit..."
trap "kill $RABBITMQ_PID $VISUALIZATION_PID" EXIT  # Clean up on exit
wait  # Wait indefinitely until the script is interrupted
