
import os
import sys
import time
import json
import pika
import infofile
from connect import connect_to_rabbitmq
from constants import SAMPLES, PATH, TASK_QUEUE
import requests
import logging

# Configure logging to output to the console with a basic format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def check_file_exists(file_path):
    """
    Check if a file exists at a given URL.
    
    Args:
        file_path (str): The URL of the file to check.
    
    Returns:
        bool: True if the file exists, False otherwise.
    """
    try:
        # Send a HEAD request to check if the file exists
        response = requests.head(file_path)
        return response.status_code == 200
    except Exception as e:
        logging.error(f"Error checking file existence at {file_path}: {e}")
        return False

def main():
    """
    Main function to distribute processing tasks to the RabbitMQ queue.
    
    This function connects to RabbitMQ, creates tasks for each sample, and sends them to the task queue.
    It skips samples whose files do not exist and logs the progress.
    """
    # Connect to RabbitMQ
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    
    # Declare the task queue as durable to ensure message persistence
    channel.queue_declare(queue=TASK_QUEUE, durable=True)
    
    # Set message persistence properties
    properties = pika.BasicProperties(
        delivery_mode=2,  # Make message persistent
    )
    
    # Get analysis parameters from environment variables
    lumi = float(os.environ.get('LUMI', '10'))
    fraction = float(os.environ.get('FRACTION', '1.0'))
    
    logging.info(f"Starting data loader with lumi={lumi}, fraction={fraction}")
    
    # Create and send tasks for each sample
    task_count = 0
    for sample_type, sample_info in SAMPLES.items():
        for sample_name in sample_info['list']:
            # Create the file path based on the sample type
            if sample_type == 'data':
                prefix = "Data/"
                file_path = PATH + prefix + sample_name + ".4lep.root"
            else:
                prefix = "MC/mc_" + str(infofile.infos[sample_name]["DSID"]) + "."
                file_path = PATH + prefix + sample_name + ".4lep.root"
            
            # Skip the sample if the file does not exist
            if not check_file_exists(file_path):
                logging.warning(f"File not found: {file_path}")
                continue
            
            # Create a task dictionary for the sample
            task = {
                'sample_type': sample_type,
                'sample_name': sample_name,
                'lumi': lumi,
                'fraction': fraction
            }
            
            # Send the task to the task queue
            channel.basic_publish(
                exchange='',
                routing_key=TASK_QUEUE,
                body=json.dumps(task),
                properties=properties
            )
            
            logging.info(f"Sent task for {sample_type} - {sample_name}")
            task_count += 1
    
    logging.info(f"Sent {task_count} tasks to the queue")
    
    # Close the RabbitMQ connection
    connection.close()

if __name__ == "__main__":
    main()