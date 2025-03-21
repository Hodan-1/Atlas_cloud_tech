import pika
import os
import time
import json
import cloudpickle
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_to_rabbitmq(max_retries=10, retry_delay=5):
    """Connect to RabbitMQ with retries"""
    host = os.environ.get('RABBITMQ_HOST')
    user = os.environ.get('RABBITMQ_USER', 'user')
    password = os.environ.get('RABBITMQ_PASS', 'password')
    
    credentials = pika.PlainCredentials(user, password)
    parameters = pika.ConnectionParameters(
        host=host,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    
    for attempt in range(max_retries):
        try:
            connection = pika.BlockingConnection(parameters)
            logger.info(f"Connected to RabbitMQ at {host}")
            return connection
        except pika.exceptions.AMQPConnectionError as e:
            if attempt < max_retries - 1:
                logger.warning(f"Failed to connect to RabbitMQ. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect to RabbitMQ after multiple attempts")
                raise

# Queue names
SAMPLE_QUEUE = 'sample_queue'
PROCESS_QUEUE = 'process_queue'
ANALYSIS_QUEUE = 'analysis_queue'
PLOT_QUEUE = 'plot_queue'
RESULT_QUEUE = 'result_queue'

def serialize_data(data):
    """Serialize complex data structures"""
    return cloudpickle.dumps(data)

def deserialize_data(data):
    """Deserialize complex data structures"""
    return cloudpickle.loads(data)