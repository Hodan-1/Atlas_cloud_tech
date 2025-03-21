FROM python:3.12

# Set the working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose necessary ports (optional)
EXPOSE 5000

# Default command (overridden in docker-compose)
CMD ["python", "task_worker.py"]