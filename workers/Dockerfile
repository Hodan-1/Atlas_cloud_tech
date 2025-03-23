# Use the official Python 3.10 image as the base image
FROM python:3.10

# Set the working directory inside the container to /app
WORKDIR /app

# Install system dependencies required for building and running the application
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \  
    zlib1g-dev \  
    && apt-get clean \ 
    && rm -rf /var/lib/apt/lists/*  
    
# Copy the requirements.txt and infofile.py files to the working directory
COPY requirements.txt .
COPY infofile.py .

# Install Python dependencies listed in requirements.txt
# --no-cache-dir avoids storing downloaded packages locally to reduce image size
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code to the working directory
COPY . .

# Set Python to run in optimized mode (removes assert statements and debug information)
ENV PYTHONOPTIMIZE=1

# Set default environment variables
ENV MAX_WORKERS=4 
ENV PYTHONUNBUFFERED=1 

# Set the default command to run the data processor worker script
CMD ["python", "/app/workers/data_processor.py"]
