#!/bin/bash

set -e

# Build the data-loader image
echo "Building data-loader image..."
docker build -t data-loader:latest workers/

# Build the data-processor image
echo "Building data-processor image..."
docker build -t data-processor:latest workers/

# Build the analysis image
echo "Building analysis image..."
docker build -t analysis:latest workers/

# Build the visualization image
echo "Building visualization image..."
docker build -t visualization:latest workers/

# Optionally, push the images to Docker Hub (if you want to)
# docker push data-loader:latest
# docker push data-processor:latest
# docker push analysis:latest
# docker push visualization:latest

echo "Images built!"
