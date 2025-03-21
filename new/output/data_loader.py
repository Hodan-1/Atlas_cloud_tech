import sys
import json
import time
import uproot
import numpy as np
import infofile
import pika
import logging
from common import connect_to_rabbitmq, serialize_data
from common import SAMPLE_QUEUE, PROCESS_QUEUE
from constants import ATLAS_PATH, SAMPLES

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)  # Log to console
    ]
)

def load_file(sample_type, sample_name):
    """Load a ROOT file and return the tree"""
    if sample_type == 'data':
        prefix = "Data/"
        file_path = ATLAS_PATH + prefix + sample_name + ".4lep.root"
    else:
        prefix = "MC/mc_" + str(infofile.infos[sample_name]["DSID"]) + "."
        file_path = ATLAS_PATH + prefix + sample_name + ".4lep.root"
    
    logging.info(f"Loading file: {file_path}")
    return uproot.open(file_path + ":mini")

def callback(ch, method, properties, body):
    """Process a sample loading task from the queue"""
    try:
        # Parse task
        task = json.loads(body.decode())
        sample_type = task['sample_type']
        sample_name = task['sample_name']
        task_id = task['task_id']
        lumi = task.get('lumi', 10)
        fraction = task.get('fraction', 1.0)
        
        logging.info(f"Processing {sample_type} sample: {sample_name}")
        start_time = time.time()
        
        # Load the ROOT file
        tree = load_file(sample_type, sample_name)
        
        # Create a processing task
        process_task = {
            'task_id': task_id,
            'sample_type': sample_type,
            'sample_name': sample_name,
            'tree': serialize_data(tree),
            'lumi': lumi,
            'fraction': fraction,
            'is_mc': sample_type != 'data'
        }
        
        # Send to processing queue
        channel = ch.connection.channel()
        channel.queue_declare(queue=PROCESS_QUEUE, durable=True)
        channel.basic_publish(
            exchange='',
            routing_key=PROCESS_QUEUE,
            body=json.dumps(process_task, default=str),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                content_type='application/json'
            )
        )
        
        elapsed = time.time() - start_time
        logging.info(f"Loaded {sample_name} in {round(elapsed, 1)}s")
        
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        # Notify that loading is done
        logging.info(f"Loading completed for {sample_name}. Task sent to processing queue.")
        
    except Exception as e:
        logging.error(f"Error processing sample {task['sample_name']}: {e}")
        # Acknowledge the message even on error to avoid reprocessing
        ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    """Main function to process sample loading tasks from the queue"""
    try:
        # Connect to RabbitMQ
        connection = connect_to_rabbitmq()
        channel = connection.channel()
        
        # Declare queue
        channel.queue_declare(queue=SAMPLE_QUEUE, durable=True)
        
        # Set prefetch count
        channel.basic_qos(prefetch_count=1)
        
        # Set up consumer
        channel.basic_consume(queue=SAMPLE_QUEUE, on_message_callback=callback)
        
        logging.info("Data loader worker started. Waiting for tasks...")
        
        # Start consuming
        channel.start_consuming()
    except Exception as e:
        logging.error(f"Error in data loader main: {e}")
    finally:
        if 'connection' in locals() and connection.is_open:
            connection.close()
            logging.info("RabbitMQ connection closed.")

if __name__ == "__main__":
    main()