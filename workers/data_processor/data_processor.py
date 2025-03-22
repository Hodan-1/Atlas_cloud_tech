
# workers/data_processor/data_processor.py
import os
import sys
import json
import pika
import uproot
import awkward as ak
import vector
import numpy as np

# Add common directory to path
sys.path.append('/app')
import infofile
from common import connect_to_rabbitmq, serialize_awkward
from common.constants import PATH, VARIABLES, WEIGHT_VARIABLES, TASK_QUEUE, RESULT_QUEUE, MeV, GeV

def load_file(sample_type, sample_name):
    """Load a ROOT file and return the tree"""
    if sample_type == 'data':
        prefix = "Data/"
        file_path = PATH + prefix + sample_name + ".4lep.root"
    else:
        prefix = "MC/mc_" + str(infofile.infos[sample_name]["DSID"]) + "."
        file_path = PATH + prefix + sample_name + ".4lep.root"
    
    return uproot.open(file_path + ":mini")

def cut_lep_type(lep_type):
    """Cut lepton type (electron type is 11, muon type is 13)"""
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    lep_type_cut_bool = (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)
    return lep_type_cut_bool  # True means we should remove this entry

def cut_lep_charge(lep_charge):
    """Cut lepton charge"""
    sum_lep_charge = lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0
    return sum_lep_charge  # True means we should remove this entry

def calc_mass(lep_pt, lep_eta, lep_phi, lep_E):
    """Calculate invariant mass of the 4-lepton state"""
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_E})
    invariant_mass = (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M * MeV
    return invariant_mass

def calc_weight(weight_variables, sample, events, lumi=10):
    """Calculate event weights for MC samples"""
    info = infofile.infos[sample]
    xsec_weight = (lumi * 1000 * info["xsec"]) / (info["sumw"] * info["red_eff"])  # *1000 to go from fb-1 to pb-1
    total_weight = xsec_weight
    for variable in weight_variables:
        total_weight = total_weight * events[variable]
    return total_weight

def process_data(tree, sample_name, is_mc=False, lumi=10, fraction=1.0):
    """Process data from a ROOT file"""
    sample_data = []
    
    # Iterate through the tree
    for data in tree.iterate(VARIABLES + (WEIGHT_VARIABLES if is_mc else []), 
                            library="ak", 
                            entry_stop=tree.num_entries * fraction,
                            step_size=1000000):
        # Cuts
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
        return ak.concatenate(sample_data)
    else:
        return None

def callback(ch, method, properties, body):
    """Process a task from the queue"""
    try:
        # Parse task
        task = json.loads(body.decode())
        print(f"Processing {task['sample_type']} - {task['sample_name']}")
        
        # Load file
        tree = load_file(task['sample_type'], task['sample_name'])
        
        # Process data
        is_mc = task['sample_type'] != 'data'
        processed_data = process_data(
            tree, 
            task['sample_name'], 
            is_mc, 
            task['lumi'], 
            task['fraction']
        )
        
        # Create result
        result = {
            'sample_type': task['sample_type'],
            'sample_name': task['sample_name'],
            'data': serialize_awkward(processed_data),
            'error': None
        }
        
        # Send result to result queue
        connection = connect_to_rabbitmq()
        channel = connection.channel()
        channel.queue_declare(queue=RESULT_QUEUE, durable=True)
        
        # Set message persistence
        properties = pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        )
        
        channel.basic_publish(
            exchange='',
            routing_key=RESULT_QUEUE,
            body=json.dumps(result),
            properties=properties
        )
        
        connection.close()
        
        print(f"Processed {task['sample_type']} - {task['sample_name']}")
        
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        print(f"Error processing task: {e}")
        # Acknowledge the message even on error to avoid reprocessing
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
        # Send error result
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
                delivery_mode=2,  # make message persistent
            )
            
            channel.basic_publish(
                exchange='',
                routing_key=RESULT_QUEUE,
                body=json.dumps(result),
                properties=properties
            )
            
            connection.close()
        except:
            pass

def main():
    """Main function to process tasks from the queue"""
    # Connect to RabbitMQ
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    
    # Declare queue
    channel.queue_declare(queue=TASK_QUEUE, durable=True)
    
    # Set prefetch count
    channel.basic_qos(prefetch_count=1)
    
    # Set up consumer
    channel.basic_consume(queue=TASK_QUEUE, on_message_callback=callback)
    
    print("Data processor worker started. Waiting for tasks...")
    
    # Start consuming
    channel.start_consuming()

if __name__ == "__main__":
    main()
