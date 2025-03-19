# Use a lightweight Python 3.12 image
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first (for better caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy only the necessary Python scripts
COPY data_reader.py ./data_reader.py
COPY worker.py ./worker.py
COPY aggregator.py ./aggregator.py
COPY coordinator.py ./coordinator.py
COPY utils.py ./utils.py
COPY config.py ./config.py
COPY data_processor.py ./data_processor.py 
COPY infofile.py ./infofile.py
# Set the default command (to be overridden in docker-compose)
CMD ["python"]
