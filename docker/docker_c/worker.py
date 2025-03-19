import json
import pika
import logging
from config import RABBITMQ_HOST
from utils import connect_to_rabbitmq, declare_queue
from data_processor import process_data

logger = logging.getLogger(__name__)

# Dictionary to store chunks for each message
chunk_store = {}

def worker():
    """Worker function to process messages from RabbitMQ."""
    connection, channel = connect_to_rabbitmq(RABBITMQ_HOST)
    declare_queue(channel, "data_queue")
    declare_queue(channel, "summary_queue")

    def callback(ch, method, properties, body):
        try:
            raw_message = body.decode("utf-8")
            logger.info(f"Received raw message: {raw_message}")

            # Parse the message
            message = json.loads(raw_message)
            logger.info(f"Parsed message: {message} (Type: {type(message)})")

            if not isinstance(message, dict):
                logger.error(f"Error: Expected dictionary but got {type(message)} - {message}")
                return

            # Extract metadata
            message_id = message.get("message_id")  # Unique ID for the message
            chunk_index = message.get("chunk_index")  # Index of the chunk
            total_chunks = message.get("total_chunks")  # Total number of chunks
            chunk_data = message.get("chunk_data")  # The actual chunk data

            if not all([message_id, chunk_index, total_chunks, chunk_data]):
                logger.error(f"Invalid chunk message: {message}")
                return

            # Initialize chunk store for this message if it doesn't exist
            if message_id not in chunk_store:
                chunk_store[message_id] = {
                    "chunks": [None] * total_chunks,
                    "received_chunks": 0
                }

            # Store the chunk
            chunk_store[message_id]["chunks"][chunk_index] = chunk_data
            chunk_store[message_id]["received_chunks"] += 1

            # Check if all chunks have been received
            if chunk_store[message_id]["received_chunks"] == total_chunks:
                logger.info(f"All chunks received for message {message_id}. Reassembling...")

                # Reassemble the chunks
                full_data = "".join(chunk_store[message_id]["chunks"])
                logger.info(f"Reassembled data: {full_data}")

                # Parse the full data
                full_message = json.loads(full_data)
                logger.info(f"Parsed full message: {full_message}")

                # Extract file path, sample, and category
                file_path = full_message["file_path"]
                sample = full_message["sample"]
                category = full_message.get("category", "unknown")
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
                logger.info(f"Sending processed summary: {json_result}")
                channel.basic_publish(exchange="", routing_key="summary_queue", body=json_result)
                logger.info("Processed data and sent summary")

                # Clean up chunk store
                del chunk_store[message_id]

        except Exception as e:
            logger.error(f"Error processing message: {e}")

    channel.basic_consume(queue="data_queue", on_message_callback=callback, auto_ack=True)
    logger.info("Worker is waiting for tasks...")
    channel.start_consuming()

if __name__ == "__main__":
    worker()

