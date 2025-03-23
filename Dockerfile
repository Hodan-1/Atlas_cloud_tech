FROM python:3.10

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    zlib1g-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
COPY infofile .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set Python to run in optimized mode
ENV PYTHONOPTIMIZE=1

# Set default environment variables
ENV MAX_WORKERS=4
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "/app/workers/data_processor.py"]
