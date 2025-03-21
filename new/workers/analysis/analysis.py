# workers/analysis/analysis.py
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
from __init__ import ANALYSIS_QUEUE, PLOT_QUEUE
from constants import HIST_CONFIG, SAMPLES
from __init__ import MeV, GeV

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('analysis')

# Store processed data by sample type
processed_samples = {}

def setup_histogram_bins():
    """Set up histogram bins"""
    xmin = HIST_CONFIG['xmin']
    xmax = HIST_CONFIG['xmax']
    step_size = HIST_CONFIG['step_size']
    
    bin_edges = np.arange(start=xmin, stop=xmax+step_size, step=step_size)
    bin_centres = np.arange(start=xmin+step_size/2, stop=xmax+step_size/2, step=step_size)
    return bin_edges, bin_centres

def callback(ch, method, properties, body):
    """Process messages from the queue"""
    try:
        # Parse message
        message = json.loads(body)
        file_name = message['file']
        sample_type = message['sample_type']
        lumi = message['lumi']
        fraction = message['fraction']
        
        logger.info(f"Received processed {file_name} ({sample_type})")
        
        # Deserialize data
        data = deserialize_data(message['data'])
        
        # Store processed data by sample type
        processed_samples[sample_type] = data
        
        logger.info(f"Stored {len(data)} events for {sample_type}")
        
        # Check if we have all sample types
        if set(processed_samples.keys()) == set(SAMPLES.keys()):
            logger.info("All samples received. Preparing for visualization...")
            
            # Set up histogram bins
            bin_edges, bin_centres = setup_histogram_bins()
            
            # Serialize results for visualization
            serialized_results = serialize_data(processed_samples)
            
            # Send to visualization
            ch.basic_publish(
                exchange='',
                routing_key=PLOT_QUEUE,
                body=json.dumps({
                    'results': serialized_results,
                    'bin_edges': bin_edges.tolist(),
                    'bin_centres': bin_centres.tolist(),
                    'lumi': lumi,
                    'fraction': fraction,
                    'timestamp': time.time()
                }),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                )
            )
            
            logger.info("Sent results to visualization")
            
            # Clear processed samples for next batch
            processed_samples.clear()
        
        # Acknowledge message
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error analyzing message: {e}")
        import traceback
        traceback.print_exc()
        # Reject message
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def main():
    """Main function to analyze data"""
    # Wait for RabbitMQ to be ready
    time.sleep(20)
    
    # Connect to RabbitMQ
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    
    # Declare queues
    channel.queue_declare(queue=ANALYSIS_QUEUE, durable=True)
    channel.queue_declare(queue=PLOT_QUEUE, durable=True)
    
    # Set prefetch count
    channel.basic_qos(prefetch_count=1)
    
    # Set up consumer
    channel.basic_consume(queue=ANALYSIS_QUEUE, on_message_callback=callback)
    
    logger.info("Analysis worker started. Waiting for messages...")
    channel.start_consuming()

if __name__ == "__main__":
    main()
