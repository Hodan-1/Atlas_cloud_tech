import json
import uproot
import awkward as ak
import numpy as np
import vector
import pika
from calc_utils import cut_lep_type, cut_lep_charge, calc_mass, calc_weight
import infofile
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Constants
lumi = 10
MeV = 0.001
output_dir = "/app/shared_storage"
os.makedirs(output_dir, exist_ok=True)

def process_mc(sample, sample_type):
    """
    Process MC data for a given sample.
    
    Args:
        sample (str): The sample name to process.
        sample_type (str): The type of sample (e.g., 'background', 'signal').
    """
    file_path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"
    dsid = infofile.infos[sample]["DSID"]
    try:
        # Open the ROOT file
        tree = uproot.open(f"{file_path}MC/mc_{dsid}.{sample}.4lep.root:mini")
        logger.info(f"Successfully opened file: {file_path}")
    except Exception as e:
        logger.error(f"Failed to open file {file_path}: {e}")
        return

    # Define variables to extract
    weight_vars = ["mcWeight", "scaleFactor_PILEUP", 
                   "scaleFactor_ELE", "scaleFactor_MUON", 
                   "scaleFactor_LepTRIGGER"]
    variables = ['lep_pt', 'lep_eta', 'lep_phi', 'lep_E', 'lep_charge', 'lep_type']
    processed_data = []

    # Process data in chunks
    for data in tree.iterate(variables + weight_vars, library="ak", step_size=1000000):
        try:
            # Add leading lepton transverse momenta
            data['leading_lep_pt'] = data['lep_pt'][:, 0]
            data['sub_leading_lep_pt'] = data['lep_pt'][:, 1]
            data['third_leading_lep_pt'] = data['lep_pt'][:, 2]
            data['last_lep_pt'] = data['lep_pt'][:, 3]

            # Apply cuts
            data = data[~cut_lep_type(data['lep_type'])]
            data = data[~cut_lep_charge(data['lep_charge'])]

            # Calculate invariant mass
            data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], 
                                   data['lep_phi'], data['lep_E'])
            
            # Calculate total weight
            data['totalWeight'] = calc_weight(weight_vars, sample, data)
            data['sample_type'] = sample_type

            processed_data.append(data)
            logger.info(f"Processed chunk for sample: {sample}")
        except Exception as e:
            logger.error(f"Error processing chunk for sample {sample}: {e}")
            continue

    # Save processed data to a Parquet file
    if processed_data:
        output_path = os.path.join(output_dir, f"raw_{sample_type}_{sample}.parquet")
        ak.to_parquet(ak.concatenate(processed_data), output_path)
        logger.info(f"Saved processed data to {output_path}")
    else:
        logger.warning(f"No processed data for sample {sample}")

def main():
    """
    Main function to consume tasks from RabbitMQ and process them.
    """
    # Connect to RabbitMQ
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
        channel = connection.channel()
        channel.queue_declare(queue='mc_background_tasks')
        channel.queue_declare(queue='mc_signal_tasks')
        logger.info("Connected to RabbitMQ and declared queues.")
    except Exception as e:
        logger.error(f"Failed to connect to RabbitMQ: {e}")
        return

    def callback(ch, method, properties, body):
        """
        Callback function to process tasks from RabbitMQ.
        """
        try:
            task = json.loads(body)
            logger.info(f"Processing task: {task}")
            process_mc(task['sample'], task['type'])
            # Acknowledge the task
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info(f"Completed task: {task}")
        except Exception as e:
            logger.error(f"Error processing task: {e}")
            # Reject the task (do not requeue it)
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

    # Start consuming tasks
    channel.basic_consume(queue='mc_background_tasks', on_message_callback=callback, auto_ack=False)
    channel.basic_consume(queue='mc_signal_tasks', on_message_callback=callback, auto_ack=False)
    logger.info("Started consuming tasks from 'mc_tasks' and 'signal_tasks' queues.")
    channel.start_consuming()

if __name__ == "__main__":
    main()