import json
import pika
import logging
from config import RABBITMQ_HOST
from utils import connect_to_rabbitmq, declare_queue
from data_processor import process_data

logger = logging.getLogger(__name__)

def worker():
    """Worker function to process messages from RabbitMQ."""
    connection, channel = connect_to_rabbitmq(RABBITMQ_HOST)
    declare_queue(channel, "data_queue")
    declare_queue(channel, "summary_queue")

def callback(ch, method, properties, body):
    try:
        raw_message = body.decode("utf-8")
        logger.info(f" Received raw message: {raw_message}")

        message = json.loads(raw_message)
        logger.info(f"Parsed message: {message} (Type: {type(message)})")

        if not isinstance(message, dict):
            logger.error(f"Error: Expected dictionary but got {type(message)} - {message}")
            return

        file_path = message["file_path"]
        sample = message["sample"]
        category = message.get("category", "unknown")  # Add category to logs
        logger.info(f"Processing {sample} (Category: {category}) from {file_path}...")

        # Validate file path
        if not file_path.startswith("https://atlas-opendata.web.cern.ch/"):
            logger.error(f"Invalid file path: {file_path}")
            return

        # Process data
        result = process_data(file_path, sample)
        if result is None or len(result["masses"]) == 0:
            logger.warning(f"No valid data for {sample}. Skipping summary.")
            return

        # Send result to aggregator
        json_result = json.dumps(result)
        logger.info(f "Sending processed summary: {json_result}")
        channel.basic_publish(exchange="", routing_key="summary_queue", body=json_result)
        logger.info("Processed data and sent summary")

    except Exception as e:
        logger.error(f" Error processing message: {e}")
        
    channel.basic_consume(queue="data_queue", on_message_callback=callback, auto_ack=True)
    logger.info("Worker is waiting for tasks...")
    channel.start_consuming()

if __name__ == "__main__":
    worker()

