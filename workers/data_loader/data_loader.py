
# workers/data_loader/data_loader.py
import os
import sys
import time
import json
import pika

# Add common directory to path
sys.path.append('/app')
import infofile
from common import connect_to_rabbitmq
from common.constants import SAMPLES, PATH, TASK_QUEUE

import requests

def check_file_exists(file_path):
    """Check if a file exists at a URL."""
    try:
        response = requests.head(file_path)
        return response.status_code == 200
    except:
        return False


def main():
    """Main function to distribute processing tasks"""
    # Connect to RabbitMQ
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    
    # Declare queue
    channel.queue_declare(queue=TASK_QUEUE, durable=True)
    
    # Set message persistence
    properties = pika.BasicProperties(
        delivery_mode=2,  # make message persistent
    )
    
    # Get parameters from environment variables
    lumi = float(os.environ.get('LUMI', '10'))
    fraction = float(os.environ.get('FRACTION', '1.0'))
    
    print(f"Starting data loader with lumi={lumi}, fraction={fraction}")
    
    # Create and send tasks for each sample
    task_count = 0
    for sample_type, sample_info in SAMPLES.items():
        for sample_name in sample_info['list']:
            # Create file path to check if file exists
            if sample_type == 'data':
                prefix = "Data/"
                file_path = PATH + prefix + sample_name + ".4lep.root"
            else:
                prefix = "MC/mc_" + str(infofile.infos[sample_name]["DSID"]) + "."
                file_path = PATH + prefix + sample_name + ".4lep.root"
            
            # Skip if file doesn't exist
            if not check_file_exists(file_path):
                print(f"File not found: {file_path}")
                continue
            
            # Create task
            task = {
                'sample_type': sample_type,
                'sample_name': sample_name,
                'lumi': lumi,
                'fraction': fraction
            }
            
            # Send task to queue
            channel.basic_publish(
                exchange='',
                routing_key=TASK_QUEUE,
                body=json.dumps(task),
                properties=properties
            )
            
            print(f"Sent task for {sample_type} - {sample_name}")
            task_count += 1
    
    print(f"Sent {task_count} tasks to the queue")
    
    # Close connection
    connection.close()

if __name__ == "__main__":
    main()
