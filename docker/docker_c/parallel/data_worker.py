import json
import uproot
import awkward as ak
import pika
from calc_utils import cut_lep_type, cut_lep_charge, calc_mass
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Constants
MeV = 0.001
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')  # Added environment variable support
DATA_BASE_PATH = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"

def process_data(sample):
    """Process data sample with proper error handling and resource management"""
    file_path = f"{DATA_BASE_PATH}Data/{sample}.4lep.root"
    processed_chunks = []
    
    try:
        with uproot.open(file_path + ":mini") as tree:
            variables = ['lep_pt', 'lep_eta', 'lep_phi', 'lep_E', 'lep_charge', 'lep_type']
            
            for chunk in tree.iterate(variables, library="ak", step_size=1000000):
                try:
                    chunk = process_chunk(chunk)
                    processed_chunks.append(chunk)
                    logger.info(f"Processed chunk for {sample}")
                except Exception as e:
                    logger.error(f"Failed chunk processing for {sample}: {str(e)}")
                    continue

            if not processed_chunks:
                logger.warning(f"No valid data processed for {sample}")
                return None
                
            return ak.concatenate(processed_chunks)

    except Exception as e:
        logger.error(f"Failed to process {sample}: {str(e)}")
        return None

def process_chunk(chunk):
    """Apply physics transformations to a single chunk"""
    # Add pT variables
    chunk = chunk.__setitem__('leading_lep_pt', chunk['lep_pt'][:, 0])
    chunk = chunk.__setitem__('sub_leading_lep_pt', chunk['lep_pt'][:, 1])
    chunk = chunk.__setitem__('third_leading_lep_pt', chunk['lep_pt'][:, 2])
    chunk = chunk.__setitem__('last_lep_pt', chunk['lep_pt'][:, 3])

    # Apply cuts
    chunk = chunk[~cut_lep_type(chunk['lep_type'])]
    chunk = chunk[~cut_lep_charge(chunk['lep_charge'])]
    
    # Calculate invariant mass
    chunk = chunk.__setitem__('mass', calc_mass(
        chunk['lep_pt'], 
        chunk['lep_eta'], 
        chunk['lep_phi'], 
        chunk['lep_E']
    ))
    
    return chunk

def callback(ch, method, properties, body):
    """Enhanced task processing with proper error handling"""
    try:
        task = json.loads(body)
        sample = task['sample']
        logger.info(f"Starting processing for {sample}")

        result = process_data(sample)
        if result is None:
            raise ValueError(f"No valid data processed for {sample}")

        # Send results
        send_result(ch, sample, result)
        logger.info(f"Completed processing for {sample}")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        logger.error(f"Failed processing {sample}: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def send_result(channel, sample, data):
    """Safely send results with compression"""
    try:
        message = {
            'type': 'data',
            'sample': sample,
            'data': ak.to_list(data['mass']),
            'pt_variables': {
                'leading': ak.to_list(data['leading_lep_pt']),
                'sub_leading': ak.to_list(data['sub_leading_lep_pt']),
                'third': ak.to_list(data['third_leading_lep_pt']),
                'last': ak.to_list(data['last_lep_pt'])
            }
        }
        
        channel.basic_publish(
            exchange='',
            routing_key='results_queue',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Persistent messages
                headers={'sample_type': 'data'}
            )
        )
    except Exception as e:
        logger.error(f"Failed to send results for {sample}: {str(e)}")
        raise

def main():
    """RabbitMQ connection setup with proper cleanup"""
    connection = None
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(RABBITMQ_HOST)
        channel = connection.channel()
        
        # Queue declarations
        channel.queue_declare(queue='data_tasks', durable=True)
        channel.queue_declare(queue='results_queue', durable=True)
        
        # Fair dispatch
        channel.basic_qos(prefetch_count=1)
        
        channel.basic_consume(
            queue='data_tasks',
            on_message_callback=callback,
            auto_ack=False
        )
        
        logger.info("Data worker started. Waiting for tasks...")
        channel.start_consuming()

    except Exception as e:
        logger.error(f"RabbitMQ connection failed: {str(e)}")
    finally:
        if connection and connection.is_open:
            connection.close()

if __name__ == "__main__":
    main()
