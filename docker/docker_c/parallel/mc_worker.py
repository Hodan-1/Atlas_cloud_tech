import json
import uproot
import awkward as ak
import pika
from calc_utils import cut_lep_type, cut_lep_charge, calc_mass, calc_weight
import infofile
import logging
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Constants
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
BASE_PATH = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"

def process_mc(sample, sample_type):
    """Process MC data and send to appropriate queue"""
    try:
        dsid = infofile.infos[sample]["DSID"]
        file_url = f"{BASE_PATH}MC/mc_{dsid}.{sample}.4lep.root"
        
        with uproot.open(f"{file_url}:mini") as tree:
            # Process in chunks
            processed_chunks = []
            for chunk in tree.iterate(library="ak", step_size=1000000):
                processed = process_chunk(chunk, sample)
                if processed is not None:
                    processed_chunks.append(processed)
            
            if processed_chunks:
                send_processed_data(
                    ak.concatenate(processed_chunks),
                    sample,
                    sample_type
                )

    except Exception as e:
        logger.error(f"Failed processing {sample}: {str(e)}")

def process_chunk(chunk, sample):
    """Process individual chunk with physics transformations"""
    try:
        # Add pT variables
        chunk = add_pt_variables(chunk)
        
        # Apply cuts
        chunk = chunk[~cut_lep_type(chunk['lep_type'])]
        chunk = chunk[~cut_lep_charge(chunk['lep_charge'])]
        
        # Calculate physics properties
        chunk['mass'] = calc_mass(
            chunk['lep_pt'], 
            chunk['lep_eta'],
            chunk['lep_phi'],
            chunk['lep_E']
        )
        
        # Calculate weights for MC
        if 'data' not in sample:
            chunk['totalWeight'] = calc_weight(
                ["mcWeight", "scaleFactor_PILEUP",
                 "scaleFactor_ELE", "scaleFactor_MUON",
                 "scaleFactor_LepTRIGGER"],
                sample,
                chunk
            )
            
        return chunk
    
    except Exception as e:
        logger.error(f"Chunk processing failed: {str(e)}")
        return None

def add_pt_variables(chunk):
    """Add pT-related variables to chunk"""
    return chunk.__setitem__(
        'leading_lep_pt', chunk['lep_pt'][:, 0]
    ).__setitem__(
        'sub_leading_lep_pt', chunk['lep_pt'][:, 1]
    ).__setitem__(
        'third_leading_lep_pt', chunk['lep_pt'][:, 2]
    ).__setitem__(
        'last_lep_pt', chunk['lep_pt'][:, 3]
    )

def send_processed_data(data, sample, sample_type):
    """Send processed data to appropriate queue"""
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(RABBITMQ_HOST))
        channel = connection.channel()
        
        # Determine correct queue
        queue_name = 'mc_signal_tasks' if 'Signal' in sample_type else 'mc_background_tasks'
        
        message = {
            'sample': sample,
            'type': sample_type,
            'mass': ak.to_list(data['mass']),
            'weights': ak.to_list(data.get('totalWeight', [1.0]*len(data))),
            'pt_variables': {
                'leading': ak.to_list(data['leading_lep_pt']),
                'sub_leading': ak.to_list(data['sub_leading_lep_pt']),
                'third': ak.to_list(data['third_leading_lep_pt']),
                'last': ak.to_list(data['last_lep_pt'])
            }
        }
        
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Persistent
                headers={'sample_type': sample_type}
            )
        )
        
        connection.close()
        logger.info(f"Sent {len(data)} events from {sample} to {queue_name}")

    except Exception as e:
        logger.error(f"Failed to send {sample}: {str(e)}")

def consume_processed_mc():
    """Aggregate data and generate plots"""
    aggregated_data = {
        'background': {'mass': [], 'weights': []},
        'signal': {'mass': [], 'weights': []}
    }

    def callback(ch, method, properties, body):
        try:
            message = json.loads(body)
            sample_type = 'signal' if 'Signal' in message['type'] else 'background'
            
            # Aggregate data
            aggregated_data[sample_type]['mass'].extend(message['mass'])
            aggregated_data[sample_type]['weights'].extend(message['weights'])
            
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            # Check completion (implementation needed)
            if check_completion():
                generate_plots(aggregated_data)

        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    
    # Setup queues
    channel.queue_declare(queue='mc_background_tasks', durable=True)
    channel.queue_declare(queue='mc_signal_tasks', durable=True)
    
    # Start consuming
    channel.basic_consume(queue='mc_background_tasks', on_message_callback=callback)
    channel.basic_consume(queue='mc_signal_tasks', on_message_callback=callback)
    
    logger.info("Started consuming processed MC data")
    channel.start_consuming()

def generate_plots(data):
    """Your original plotting code goes here"""
    logger.info("Generating plots from aggregated data")
    # Implement your plotting logic using data['background'] and data['signal']

if __name__ == "__main__":
    # Example processing
    process_mc('Zee', 'Background Z+ttbar')
    process_mc('ggH125_ZZ4lep', 'Signal (m_H = 125 GeV)')
    consume_processed_mc()