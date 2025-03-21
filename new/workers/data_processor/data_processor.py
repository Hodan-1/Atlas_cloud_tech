# workers/data_processor/data_processor.py
import os
import sys
import json
import time
import logging
import pika
import numpy as np
import awkward as ak

# Add app directory to path
sys.path.append('/app')
from __init__ import connect_to_rabbitmq, serialize_data, deserialize_data
from __init__ import PROCESS_QUEUE, ANALYSIS_QUEUE
from __init__ import MeV, GeV

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('data_processor')

def process_data(data, sample_type):
    """Process physics data"""
    logger.info(f"Processing {sample_type} data with {len(data)} events")
    
    # Apply selection: only events with at least 2 leptons
    mask = ak.num(data.lepton_pt) >= 2
    filtered_data = data[mask]
    
    logger.info(f"Selected {len(filtered_data)}/{len(data)} events with at least 2 leptons")
    
    # Return processed data
    return filtered_data

def callback(ch, method, properties, body):
    """Process messages from the queue"""
    try:
        # Parse message
        message = json.loads(body)
        file_name = message['file']
        sample_type = message['sample_type']
        lumi = message['lumi']
        fraction = message['fraction']
        
        logger.info(f"Received {file_name} ({sample_type})")
        
        # Deserialize data
        data = deserialize_data(message['data'])
        
        # Process data
        processed_data = process_data(data, sample_type)
        
        # Serialize processed data
        serialized_data = serialize_data(processed_data)
        
        # Send to analysis
        ch.basic_publish(
            exchange='',
            routing_key=ANALYSIS_QUEUE,
            body=json.dumps({
                'file': file_name,
                'sample_type': sample_type,
                'data': serialized_data,
                'lumi': lumi,
                'fraction': fraction,
                'n_events_before': len(data),
                'n_events_after': len(processed_data),
                'timestamp': time.time()
            }),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )
        
        logger.info(f"Processed {file_name}: {len(processed_data)}/{len(data)} events passed selection")
        
        # Acknowledge message
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        import traceback
        traceback.print_exc()
        # Reject message
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    """Main function to process data"""
    # Wait for RabbitMQ to be ready
    time.sleep(15)
    
    # Connect to RabbitMQ
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    
    # Declare queues
    channel.queue_declare(queue=PROCESS_QUEUE, durable=True)
    channel.queue_declare(queue=ANALYSIS_QUEUE, durable=True)
    
    # Set prefetch count
    channel.basic_qos(prefetch_count=1)
    
    # Set up consumer
    channel.basic_consume(queue=PROCESS_QUEUE, on_message_callback=callback)
    
    logger.info("Data processor started. Waiting for messages...")
    channel.start_consuming()

if __name__ == "__main__":
    main()
