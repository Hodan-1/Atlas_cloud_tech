#!/bin/bash

set -e

# Build the images (uncomment if you don't build them separately)
# ./image_build.sh

# Run Docker Compose
docker-compose -f compose.yaml up -d

echo "Application running with Docker Compose!"
