import os
import sys
import json
import pika
import uproot
import awkward as ak
import vector
import numpy as np
import logging

# Add the common directory to the Python path to access shared modules
sys.path.append('/app')
import infofile
from connect import connect_to_rabbitmq, serialize_awkward
from constants import PATH, VARIABLES, WEIGHT_VARIABLES, TASK_QUEUE, RESULT_QUEUE, MeV, GeV

# Configure logging to output to the console with a basic format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_file(sample_type, sample_name):
    """
    Load a ROOT file and return the tree.
    
    Args:
        sample_type (str): The type of sample ('data' or 'MC').
        sample_name (str): The name of the sample.
    
    Returns:
        uproot.TTree: The ROOT tree from the file.
    """
    # Construct the file path based on the sample type
    if sample_type == 'data':
        prefix = "Data/"
        file_path = PATH + prefix + sample_name + ".4lep.root"
    else:
        prefix = "MC/mc_" + str(infofile.infos[sample_name]["DSID"]) + "."
        file_path = PATH + prefix + sample_name + ".4lep.root"
    
    logging.debug(f"Loading file: {file_path}")
    return uproot.open(file_path + ":mini")

def cut_lep_type(lep_type):
    """
    Apply a cut on lepton type (electron type is 11, muon type is 13).
    
    Args:
        lep_type (ak.Array): Array of lepton types.
    
    Returns:
        ak.Array: Boolean array indicating which entries to remove.
    """
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    lep_type_cut_bool = (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)
    logging.debug("Applied lepton type cut.")
    return lep_type_cut_bool  # True means we should remove this entry

def cut_lep_charge(lep_charge):
    """
    Apply a cut on lepton charge.
    
    Args:
        lep_charge (ak.Array): Array of lepton charges.
    
    Returns:
        ak.Array: Boolean array indicating which entries to remove.
    """
    sum_lep_charge = lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0
    logging.debug("Applied lepton charge cut.")
    return sum_lep_charge  # True means we should remove this entry

def calc_mass(lep_pt, lep_eta, lep_phi, lep_E):
    """
    Calculate the invariant mass of the 4-lepton state.
    
    Args:
        lep_pt (ak.Array): Array of lepton transverse momenta.
        lep_eta (ak.Array): Array of lepton pseudorapidities.
        lep_phi (ak.Array): Array of lepton azimuthal angles.
        lep_E (ak.Array): Array of lepton energies.
    
    Returns:
        ak.Array: Array of invariant masses.
    """
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_E})
    invariant_mass = (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M * MeV
    logging.debug("Calculated invariant mass.")
    return invariant_mass

def calc_weight(weight_variables, sample, events, lumi=10):
    """
    Calculate event weights for MC samples.
    
    Args:
        weight_variables (list): List of weight variables.
        sample (str): The name of the sample.
        events (ak.Array): Array of events.
        lumi (float): Integrated luminosity in fb^-1.
    
    Returns:
        ak.Array: Array of total event weights.
    """
    info = infofile.infos[sample]
    xsec_weight = (lumi * 1000 * info["xsec"]) / (info["sumw"] * info["red_eff"])  # *1000 to go from fb-1 to pb-1
    total_weight = xsec_weight
    for variable in weight_variables:
        total_weight = total_weight * events[variable]
    logging.debug("Calculated event weights.")
    return total_weight

def process_data(tree, sample_name, is_mc=False, lumi=10, fraction=1.0):
    """
    Process data from a ROOT file.
    
    Args:
        tree (uproot.TTree): The ROOT tree to process.
        sample_name (str): The name of the sample.
        is_mc (bool): Whether the sample is MC or data.
        lumi (float): Integrated luminosity in fb^-1.
        fraction (float): Fraction of events to process.
    
    Returns:
        ak.Array: Processed data as an awkward array.
    """
    sample_data = []
    
    # Iterate through the tree in chunks
    for data in tree.iterate(VARIABLES + (WEIGHT_VARIABLES if is_mc else []), 
                            library="ak", 
                            entry_stop=tree.num_entries * fraction,
                            step_size=1000000):
        # Apply cuts
        lep_type = data['lep_type']
        data = data[~cut_lep_type(lep_type)]
        lep_charge = data['lep_charge']
        data = data[~cut_lep_charge(lep_charge)]
        
        # Calculate invariant mass
        data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_E'])
        
        # Calculate weights for MC samples
        if is_mc:
            data['totalWeight'] = calc_weight(WEIGHT_VARIABLES, sample_name, data, lumi)
        
        sample_data.append(data)
    
    # Concatenate all data chunks
    if sample_data:
        logging.info(f"Processed data for sample: {sample_name}")
        return ak.concatenate(sample_data)
    else:
        logging.warning(f"No data processed for sample: {sample_name}")
        return None

def callback(ch, method, properties, body):
    """
    Callback function to process a task from the queue.
    
    Args:
        ch: The RabbitMQ channel.
        method: The delivery method.
        properties: The message properties.
        body: The message body.
    """
    try:
        # Parse the task from the message body
        task = json.loads(body.decode())
        logging.info(f"Processing {task['sample_type']} - {task['sample_name']}")
        
        # Load the ROOT file
        tree = load_file(task['sample_type'], task['sample_name'])
        
        # Process the data
        is_mc = task['sample_type'] != 'data'
        processed_data = process_data(
            tree, 
            task['sample_name'], 
            is_mc, 
            task['lumi'], 
            task['fraction']
        )
        
        # Create the result dictionary
        result = {
            'sample_type': task['sample_type'],
            'sample_name': task['sample_name'],
            'data': serialize_awkward(processed_data),
            'error': None
        }
        
        # Send the result to the result queue
        connection = connect_to_rabbitmq()
        channel = connection.channel()
        channel.queue_declare(queue=RESULT_QUEUE, durable=True)
        
        # Set message persistence
        properties = pika.BasicProperties(
            delivery_mode=2,  # Make message persistent
        )
        
        channel.basic_publish(
            exchange='',
            routing_key=RESULT_QUEUE,
            body=json.dumps(result),
            properties=properties
        )
        
        connection.close()
        
        logging.info(f"Processed {task['sample_type']} - {task['sample_name']}")
        
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        logging.error(f"Error processing task: {e}")
        # Acknowledge the message even on error to avoid reprocessing
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        # Send an error result
        try:
            task = json.loads(body.decode())
            result = {
                'sample_type': task['sample_type'],
                'sample_name': task['sample_name'],
                'data': None,
                'error': str(e)
            }
            
            connection = connect_to_rabbitmq()
            channel = connection.channel()
            channel.queue_declare(queue=RESULT_QUEUE, durable=True)
            
            properties = pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            )
            
            channel.basic_publish(
                exchange='',
                routing_key=RESULT_QUEUE,
                body=json.dumps(result),
                properties=properties
            )
            
            connection.close()
        except Exception as e:
            logging.error(f"Failed to send error result: {e}")

def main():
    """
    Main function to process tasks from the queue.
    """
    # Connect to RabbitMQ
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    
    # Declare the task queue as durable
    channel.queue_declare(queue=TASK_QUEUE, durable=True)
    
    # Set prefetch count to limit the number of unacknowledged messages
    channel.basic_qos(prefetch_count=1)
    
    # Set up the consumer with the callback function
    channel.basic_consume(queue=TASK_QUEUE, on_message_callback=callback)
    
    logging.info("Data processor worker started. Waiting for tasks...")
    
    # Start consuming messages
    channel.start_consuming()

if __name__ == "__main__":
    main()