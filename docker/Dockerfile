# Use an official Python image
FROM python:3.12

# Set the working directory
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

RUN pip list | grep pika
# Copy all necessary files
COPY work.py mess.py infofile.py /app/

# No CMD here; commands are specified in docker-compose.yml
