#test functionality
import pika
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_to_rabbitmq(host, retries=5, delay=5):
    """Tries to connect to RabbitMQ with retries and sets a longer heartbeat timeout."""
    for attempt in range(retries):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host,
                    heartbeat=120,  # Increase heartbeat timeout to 120s
                    blocked_connection_timeout=300,  # Prevent connection from blocking indefinitely
                )
            )
            channel = connection.channel()
            return connection, channel
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{retries}: Failed to connect to RabbitMQ: {e}")
            if attempt < retries - 1:
                time.sleep(delay)  # Wait before retrying
            else:
                raise


def declare_queue(channel, queue_name):
    channel.queue_declare(queue=queue_name)
