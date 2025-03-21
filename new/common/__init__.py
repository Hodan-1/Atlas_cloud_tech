# common/__init__.py
import os
import pika
import json
import time
import pickle
import base64
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Queue names
DATA_QUEUE = 'data_queue'
PROCESS_QUEUE = 'process_queue'
ANALYSIS_QUEUE = 'analysis_queue'
PLOT_QUEUE = 'plot_queue'

# Physics constants
MeV = 1
GeV = 1000 * MeV

def connect_to_rabbitmq(max_retries=12, retry_delay=5):
    """Connect to RabbitMQ server with retries"""
    host = os.environ.get('RABBITMQ_HOST', 'localhost')
    user = os.environ.get('RABBITMQ_USER', 'atlas')
    password = os.environ.get('RABBITMQ_PASS', 'atlas')
    
    logging.info(f"Connecting to RabbitMQ at {host}")
    
    for attempt in range(max_retries):
        try:
            credentials = pika.PlainCredentials(user, password)
            parameters = pika.ConnectionParameters(
                host=host,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            connection = pika.BlockingConnection(parameters)
            logging.info("Successfully connected to RabbitMQ")
            return connection
        except Exception as e:
            logging.warning(f"Failed to connect to RabbitMQ (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logging.error("Max retries reached. Could not connect to RabbitMQ.")
                raise

def serialize_data(data):
    """Serialize data for transmission"""
    try:
        serialized = base64.b64encode(pickle.dumps(data)).decode('utf-8')
        return serialized
    except Exception as e:
        logging.error(f"Error serializing data: {e}")
        raise

def deserialize_data(serialized_data):
    """Deserialize data received from queue"""
    try:
        data = pickle.loads(base64.b64decode(serialized_data))
        return data
    except Exception as e:
        logging.error(f"Error deserializing data: {e}")
        raise
