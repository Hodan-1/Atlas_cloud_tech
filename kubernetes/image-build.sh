#!/bin/bash

set -e

# Build the RabbitMQ image (if you're customizing it)
# docker build -t rabbitmq:latest .  # If you have a custom RabbitMQ Dockerfile

# Build the data-loader image
docker build -t data-loader:latest ../workers/data_loader

# Build the data-processor image
docker build -t data-processor:latest ../workers/data_processor

# Build the analysis image
docker build -t analysis:latest ../workers/analysis

# Build the visualization image
docker build -t visualization:latest ../workers/visualization

# Optionally, push the images to Docker Hub (if you want to)
# docker push data-loader:latest
# docker push data-processor:latest
# docker push analysis:latest
# docker push visualization:latest

echo "Images built successfully!"
