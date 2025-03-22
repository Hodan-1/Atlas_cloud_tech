# ATLAS Open Data Analysis Pipeline

## Description

This project implements a distributed data analysis pipeline for processing and analysing ATLAS Open Data from CERN. It utilises RabbitMQ for message queuing and can be deployed using either Docker Compose or Kubernetes. The pipeline consists of several microservices: data loader, data processor, analysis, and visualisation. A key aspect of this project is the ability to monitor and compare CPU usage across different deployment environments (Docker Compose vs. Kubernetes).

## Key Features

* **Distributed Microservices Architecture:** The pipeline is broken down into independent, scalable microservices.
* **Message Queuing with RabbitMQ:** RabbitMQ facilitates asynchronous communication between services, ensuring reliable task management and decoupling.
* **Data Processing Pipeline:**
    * **Data Loader:** Retrieves data from the ATLAS Open Data repository.
    * **Data Processor:** Performs data cleaning, transformation, and feature extraction.
    * **Analysis:** Executes statistical analysis on the processed data.
    * **Visualisation:** Generates plots and visualisations of the analysis results.
* **Flexible Deployment:** Supports deployment using both Docker Compose for local development and Kubernetes for production environments.
* **CPU Usage Monitoring:** Includes a `cpu_monitor.py` script to track CPU usage of individual services, enabling performance comparison between Docker Compose and Kubernetes deployments.
* **Automated Build and Deployment:** Provides scripts for building Docker images and deploying to both Docker Compose and Kubernetes.

## Project Structure

```
ATLAS-Open-Data-Analysis/
├── common/
│   ├── __init__.py
│   ├── constants.py
│   └── cpu_monitor.py  # CPU monitoring script
├── kubernetes/
│   ├── analysis-deployment.yaml
│   ├── data-loader-deployment.yaml
│   ├── data-processor-deployment.yaml
│   ├── rabbitmq-deployment.yaml
│   ├── rabbitmq-service.yaml
│   ├── service.yaml
│   ├── visual-deployment.yaml
│   └── visualisation-service.yaml
├── workers/
│   ├── data_loader/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── ... (data_loader code)
│   ├── data_processor/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── ... (data_processor code)
│   ├── analysis/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── ... (analysis code)
│   └── visualisation/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── ... (visualisation code)
├── analysis.py  # Main analysis script
├── build-images.sh  # Script to build Docker images
├── compose.yaml  # Docker Compose configuration file
├── docker-run.sh  # Script to run the application with Docker Compose
├── kubernetes-run.sh  # Script to deploy the application to Kubernetes
├── README.md
└── requirements.txt
```

## Getting Started

### Prerequisites

* Python 3.10
* Docker
* Docker Compose
* Kubernetes cluster (Minikube, Kind, or a cloud provider)
* `kubectl`
* RabbitMQ (Managed by Docker Compose and Kubernetes configurations)

### Installation

1. Clone the repository:

    ```bash
    git clone [your_repository_url]
    ```

2. Navigate to the project directory:

    ```bash
    cd [your_project_directory]
    ```

### Building Docker Images

Run the `build-images.sh` script to build the Docker images for each service:

```bash
./build-images.sh
```

This script builds the following images:

- `data-loader:latest`
- `data-processor:latest`
- `analysis:latest`
- `visualisation:latest`

## Docker Compose Deployment

Docker Compose is used to orchestrate the RabbitMQ message broker and the data processing microservices on a single host or a small cluster of hosts.

1. Build the Docker images (if not already done):

    ```bash
    ./build-images.sh
    ```

2. Start the pipeline using Docker Compose:

    ```bash
    ./docker-run.sh
    ```

## Kubernetes Deployment

Kubernetes is used to deploy the RabbitMQ message broker and the data processing microservices in a cluster environment, providing high availability, scalability, and fault tolerance.

1. Set the `IMAGE_TAG` and `NAMESPACE` environment variables:

    ```bash
    export IMAGE_TAG="latest"  # Or a specific tag
    export NAMESPACE="my-namespace"
    ```

2. Deploy the application to Kubernetes:

    ```bash
    ./kubernetes-run.sh
    ```

## Monitoring CPU Usage

The `cpu_monitor.py` script tracks CPU usage for each microservice:

1. Ensure the pipeline is running.
2. Execute the monitoring script:

    ```bash
    python common/cpu_monitor.py
    ```

## Configuration

Environment variables can be configured for each service:

| Variable         | Description                                      | Default Value |
|------------------|--------------------------------------------------|---------------|
| `RABBITMQ_HOST`  | RabbitMQ server hostname                         | rabbitmq      |
| `RABBITMQ_USER`  | RabbitMQ username                                | atlas         |
| `RABBITMQ_PASS`  | RabbitMQ password                                | atlas         |
| `LUMI`           | Luminosity value (used by data-loader/analysis)   | 10            |
| `FRACTION`       | Fraction of data to process (data-loader/analysis)| 1.0           |
| `PT_CUTS`        | Comma-separated list of pT cuts (data-loader)     | 20,15,10      |

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Acknowledgements

This project is inspired by the ATLAS Open Data initiative at CERN and leverages modern container orchestration tools to streamline data analysis workflows.

