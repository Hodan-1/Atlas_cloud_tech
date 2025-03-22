# common/__init__.py
import os
import time
import pika
import pickle
import base64

def connect_to_rabbitmq():
    """Connect to RabbitMQ with retry logic"""
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
    rabbitmq_user = os.environ.get('RABBITMQ_USER', 'atlas')
    rabbitmq_pass = os.environ.get('RABBITMQ_PASS', 'atlas')
    
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
    parameters = pika.ConnectionParameters(
        host=rabbitmq_host,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    
    max_retries = 10
    retry_delay = 5
    
    for i in range(max_retries):
        try:
            connection = pika.BlockingConnection(parameters)
            return connection
        except pika.exceptions.AMQPConnectionError:
            print(f"Failed to connect to RabbitMQ, retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
    
    raise Exception("Failed to connect to RabbitMQ after multiple attempts")

def serialize_awkward(data):
    """Serialize awkward array to base64 string"""
    if data is None:
        return None
    return base64.b64encode(pickle.dumps(data)).decode('utf-8')

def deserialize_awkward(data_str):
    """Deserialize awkward array from base64 string"""
    if data_str is None:
        return None
    return pickle.loads(base64.b64decode(data_str))