import sys
import pika
import json
import time
import logging
import uproot
import awkward as ak
from multiprocessing import Pool
from config import samples


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)



def connect_to_rabbitmq():
    """Connect to RabbitMQ with retries."""
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters('rabbitmq', heartbeat=600))
            logger.info("Successfully connected to RabbitMQ.")
            return connection
        except pika.exceptions.AMQPConnectionError as e:
            logger.error("Failed to connect to RabbitMQ, retrying in 5 seconds...")
            time.sleep(5)

def publish_task(channel, queue_name, task):
    """Publish a task to a RabbitMQ queue."""
    try:
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(task),
            properties=pika.BasicProperties(
            delivery_mode=2),
        )
        logger.info(f"Published task to {queue_name}: {task}")
    except Exception as e:
        logger.error(f"Failed to publish task to {queue_name}: {e}")

def publish_tasks_for_category(channel, category, queue_name, task_type):
    """Publish tasks for a specific category (e.g., data, background, signal)."""
    for sample in samples[category]['list']:
        task = {'sample': sample, 'type': task_type}
        publish_task(channel, queue_name, task)

def wait_for_queue_to_empty(channel, queue_name):
    """Wait for a RabbitMQ queue to empty."""
    while True:
        method_frame = channel.queue_declare(queue=queue_name, passive=True)
        message_count = method_frame.method.message_count
        if message_count == 0:
            logger.info(f"Queue {queue_name} is empty.")
            break
        logger.info(f"Waiting for {message_count} tasks in {queue_name} to be processed...")
        time.sleep(5)
        
def start_tasks():
    """Read all samples and send them to RabbitMQ."""
    connection = connect_to_rabbitmq()
    channel = connection.channel()

    # Declare queues
    queues = ['data_tasks', 'mc_background_tasks', 'mc_signal_tasks', 'transverse_tasks']
    for queue in queues:
        channel.queue_declare(queue=queue)
        logger.info(f"Declared queue: {queue}")

    # Publish data tasks
    publish_tasks_for_category(channel, 'data', 'data_tasks', 'data')

    # Publish MC background tasks
    for bg_category in [r'Background $Z,t\bar{t}$', r'Background $ZZ^*$']:
        publish_tasks_for_category(channel, bg_category, 'mc_background_tasks', 'background')

    # Publish signal tasks
    signal_category = r'Signal ($m_H$ = 125 GeV)'
    publish_tasks_for_category(channel, signal_category, 'mc_signal_tasks', 'signal')

    # Publish transverse task
    publish_task(channel, 'transverse_tasks', {'action': 'process'})

    # Wait for all queues to empty
    for queue in queues:
        wait_for_queue_to_empty(channel, queue)

    # Close the connection
    connection.close()
    logger.info("All tasks published. Connection closed.")

if __name__ == "__main__":
    start_tasks()