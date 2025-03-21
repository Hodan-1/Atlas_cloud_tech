# Use official Python 3.12 slim image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
COPY infofile.py .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY *.py .

# Environment variables
ENV PYTHONPATH=/app
ENV RABBITMQ_HOST=rabbitmq
ENV RABBITMQ_USER=admin
ENV RABBITMQ_PASS=admin123

# Default command (override in docker-compose)
CMD ["python", "coordinator.py"]