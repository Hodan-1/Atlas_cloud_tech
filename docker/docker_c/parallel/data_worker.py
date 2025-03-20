
import json
import uproot
import awkward as ak
import numpy as np
import vector
import pika
from calc_utils import cut_lep_type, cut_lep_charge, calc_mass
import infofile 
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
# Constant
MeV = 0.001
GeV = 1.0

output_dir = "/app/shared_storage"
os.makedirs(output_dir, exist_ok=True)

def process_data(sample):
    path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"
    file_path = f"{path}Data/{sample}.4lep.root"
    
    
    try:
        tree = uproot.open(file_path + ":mini")
    except Exception as e:
        logging.error(f"Failed to open file {file_path}: {e}")
        return
    variables = ['lep_pt','lep_eta','lep_phi','lep_E','lep_charge','lep_type']
    
    processed_data = []
    for data in tree.iterate(variables, library="ak", step_size=1000000):
        try:
            data['leading_lep_pt'] = data['lep_pt'][:,0]  
            data['sub_leading_lep_pt'] = data['lep_pt'][:,1]
            data['third_leading_lep_pt'] = data['lep_pt'][:,2]
            data['last_lep_pt'] = data['lep_pt'][:,3]

        # Apply cuts
            data = data[~cut_lep_type(data['lep_type'])]
            data = data[~cut_lep_charge(data['lep_charge'])]
            
            # Calculate mass
            data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], 
                                data['lep_phi'], data['lep_E'])
            
            processed_data.append(data)
            logger.info(f"Processed chunk for sample: {sample}")
        except Exception as e:
            logging.error(f"Error processing data: {e}")
            continue

           # Save processed data to a Parquet file
    if processed_data:
        output_path = os.path.join(output_dir, f"raw_{sample}.parquet")
        ak.to_parquet(ak.concatenate(processed_data), output_path)
        logger.info(f"Saved processed data to {output_path}")
    else:
        logger.warning(f"No processed data for sample {sample}")

def process_task(task):
    """
    Process a single task from the RabbitMQ queue.
    
    Args:
        task (dict): The task containing the sample name.
    """
    sample = task['sample']
    logger.info(f"Processing task for sample: {sample}")
    process_data(sample)  

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='data_tasks')
    logger.info("Connected to RabbitMQ and declared queue 'data_tasks'.")

    def callback(ch, method, properties, body):
        try:
            task = json.loads(body)
            process_task(task)
        except Exception as e:
            logger.error(f"Error processing task: {e}")

    channel.basic_consume(queue='data_tasks', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == "__main__":
    main()