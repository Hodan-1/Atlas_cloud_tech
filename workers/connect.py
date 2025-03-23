import os
import time
import pika
import pickle
import base64
import logging

# Configure logging to output to the console with a basic format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_to_rabbitmq():
    """
    Connect to RabbitMQ with retry logic.
    
    This function attempts to establish a connection to RabbitMQ using environment variables
    for host, user, and password. It retries the connection up to a maximum number of times
    with a delay between attempts.
    
    Returns:
        pika.BlockingConnection: A connection to RabbitMQ.
    
    Raises:
        Exception: If the connection fails after the maximum number of retries.
    """
    # Retrieve RabbitMQ connection details from environment variables
    rabbitmq_host = os.environ.get('RABBITMQ_HOST', 'rabbitmq')
    rabbitmq_user = os.environ.get('RABBITMQ_USER', 'atlas')
    rabbitmq_pass = os.environ.get('RABBITMQ_PASS', 'atlas')
    
    # Create credentials and connection parameters
    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)
    parameters = pika.ConnectionParameters(
        host=rabbitmq_host,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    
    # Define retry logic parameters
    max_retries = 10
    retry_delay = 5
    
    # Attempt to connect to RabbitMQ with retries
    for i in range(max_retries):
        try:
            connection = pika.BlockingConnection(parameters)
            logging.info("Successfully connected to RabbitMQ.")
            return connection
        except pika.exceptions.AMQPConnectionError:
            logging.warning(f"Failed to connect to RabbitMQ, retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
    
    # Raise an exception if all retries fail
    logging.error("Failed to connect to RabbitMQ after multiple attempts.")
    raise Exception("Failed to connect to RabbitMQ after multiple attempts")

def serialize_awkward(data):
    """
    Serialize an awkward array to a base64-encoded string.
    
    This function takes an awkward array, serializes it using pickle, and then encodes
    the serialized data as a base64 string.
    
    Args:
        data: The awkward array to serialize.
    
    Returns:
        str: A base64-encoded string representing the serialized awkward array, or None if the input is None.
    """
    if data is None:
        logging.debug("No data provided to serialize, returning None.")
        return None
    
    # Serialize the data and encode it as a base64 string
    serialized_data = base64.b64encode(pickle.dumps(data)).decode('utf-8')
    logging.debug("Data serialized successfully.")
    return serialized_data

def deserialize_awkward(data_str):
    """
    Deserialize an awkward array from a base64-encoded string.
    
    This function takes a base64-encoded string, decodes it, and then deserializes
    it back into an awkward array using pickle.
    
    Args:
        data_str (str): The base64-encoded string to deserialize.
    
    Returns:
        The deserialized awkward array, or None if the input is None.
    """
    if data_str is None:
        logging.debug("No data string provided to deserialize, returning None.")
        return None
    
    # Decode the base64 string and deserialize the data
    deserialized_data = pickle.loads(base64.b64decode(data_str))
    logging.debug("Data deserialized successfully.")
    return deserialized_data