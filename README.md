
```markdown
# ATLAS Open Data Analysis Pipeline

## Overview
The **ATLAS Open Data Analysis Pipeline** is a distributed system designed to process and analyze data from the ATLAS experiment at CERN. Built with a microservices architecture, this project leverages **RabbitMQ** for task distribution and supports deployment using both **Docker Compose** (for local development) and **Kubernetes** (for production environments). The pipeline is composed of four main services: **Data Loader**, **Data Processor**, **Analysis Worker**, and **Visualization Worker**, each responsible for a specific stage of the data analysis workflow.

A key feature of this project is its ability to monitor and compare CPU usage across different deployment environments, providing insights into the performance characteristics of Docker Compose versus Kubernetes deployments.

---

## Key Features
- **Distributed Microservices Architecture**: The pipeline is divided into independent, scalable services that communicate asynchronously via RabbitMQ.
- **Flexible Deployment Options**: Supports deployment using Docker Compose for local development and Kubernetes for scalable, production-grade environments.
- **Data Processing Workflow**:
  - **Data Loader**: Fetches data from the ATLAS Open Data repository and queues tasks for processing.
  - **Data Processor**: Cleans, transforms, and calculates invariant masses from the raw data.
  - **Analysis Worker**: Aggregates results and prepares data for visualization.
  - **Visualization Worker**: Generates plots and calculates signal significance.
- **CPU Usage Monitoring**: Includes a `cpu_monitor.py` script to track and compare CPU usage across services in different deployment environments.
- **Automated Build and Deployment**: Provides scripts for building Docker images and deploying the application to Docker Compose or Kubernetes.

```

---

## Getting Started

### Prerequisites
Before running the project, ensure you have the following installed:
- **Python 3.10**: [Install Python](https://www.python.org/downloads/)
- **Docker**: [Install Docker](https://docs.docker.com/get-docker/)
- **Docker Compose**: [Install Docker Compose](https://docs.docker.com/compose/install/)
- **Minikube** (for Kubernetes deployment): [Install Minikube](https://minikube.sigs.k8s.io/docs/start/)
- **kubectl** (for Kubernetes deployment): [Install kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)

---

### Installation
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-username/your-repo-name.git
   cd your-repo-name
   ```

2. **Build Docker Images**:
   Run the `build-images.sh` script to build the Docker images for all services:
   ```bash
   ./build-images.sh
   ```

3. **Set Up Minikube (for Kubernetes Deployment)**:
   Start Minikube with the desired resource allocation:
   ```bash
   minikube start --cpus=4 --memory=2200
   ```

4. **Configure Docker to Use Minikube**:
   Point your Docker CLI to Minikube's Docker daemon:
   ```bash
   eval $(minikube docker-env)
   ```

---

## Running the Application

### Using Docker Compose
1. **Start the Application**:
   Run the `docker-run.sh` script to start the application using Docker Compose:
   ```bash
   ./docker-run.sh
   ```

2. **Access RabbitMQ Management UI**:
   Open your browser and navigate to:
   ```
   http://localhost:15672
   ```
   Use the following credentials:
   - Username: `atlas`
   - Password: `atlas`

3. **Access Visualization Output**:
   Open your browser and navigate to:
   ```
   http://localhost:8080
   ```

### Using Kubernetes
1. **Run the Deployment Script**:
   Use the `deploy-k8s.sh` script to automate the deployment process:
   ```bash
   ./deploy-k8s.sh
   ```

   This script will:
   - Apply all Kubernetes manifests.
   - Verify that all pods are running.
   - Set up port forwarding for RabbitMQ and the Visualization service.

2. **Access RabbitMQ Management UI**:
   The script will output the following:
   ```
   🌐 RabbitMQ Management UI is available at:
      http://localhost:15672
      Username: atlas
      Password: atlas
   ```

3. **Access Visualization Output**:
   The script will also output:
   ```
   🌐 Visualization output is available at:
      http://localhost:8080
   ```

4. **Stop the Deployment**:
   Press `Ctrl+C` to stop port forwarding and exit the script.

---

## Monitoring CPU Usage
The `cpu_monitor.py` script allows you to track the CPU usage of each microservice, enabling performance comparisons between Docker Compose and Kubernetes deployments. To use it:
1. Ensure the pipeline is running.
2. Execute the monitoring script:
   ```bash
   python common/cpu_monitor.py
   ```

---

## Troubleshooting

### Pods Stuck in `Pending` State
If pods are stuck in the `Pending` state, check the node's resource usage:
```bash
kubectl describe nodes
```
Ensure that the resource requests and limits in your `Deployment` manifests are within the node's capacity.

### Minikube Resource Allocation
If Minikube fails to start due to insufficient resources, reduce the requested memory:
```bash
minikube start --cpus=4 --memory=2200
```

### Docker Image Build Issues
If Docker images fail to build, ensure that Docker is running and that you have sufficient disk space.

---


