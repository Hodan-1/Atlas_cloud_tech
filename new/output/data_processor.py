# workers/data_processor/processor.py
import json
import time
import numpy as np
import awkward as ak
import vector
import pika
from common.rabbitmq import connect_to_rabbitmq, serialize_data, deserialize_data
from common.rabbitmq import PROCESS_QUEUE, ANALYSIS_QUEUE
from common.constants import MeV, GeV, VARIABLES, WEIGHT_VARIABLES

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
    import infofile
    info = infofile.infos[sample]
    xsec_weight = (lumi * 1000 * info["xsec"]) / (info["sumw"] * info["red_eff"])  # *1000 to go from fb-1 to pb-1
    total_weight = xsec_weight
    for variable in weight_variables:
        total_weight = total_weight * events[variable]
    return total_weight

def process_data(tree, sample_name, is_mc=False, lumi=10, pt_cuts=None, fraction=1.0):
    """Process data from a ROOT file"""
    sample_data = []
    
    # Iterate through the tree
    for data in tree.iterate(VARIABLES + (WEIGHT_VARIABLES if is_mc else []), 
                            library="ak", 
                            entry_stop=tree.num_entries * fraction,
                            step_size=1000000):
        # Record transverse momenta
        data['leading_lep_pt'] = data['lep_pt'][:, 0]
        data['sub_leading_lep_pt'] = data['lep_pt'][:, 1]
        data['third_leading_lep_pt'] = data['lep_pt'][:, 2]
        data['last_lep_pt'] = data['lep_pt'][:, 3]
        
        # Apply pT cuts if specified
        if pt_cuts:
            data = data[data['leading_lep_pt'] * MeV > pt_cuts[0]]
            data = data[data['sub_leading_lep_pt'] * MeV > pt_cuts[1]]
            data = data[data['third_leading_lep_pt'] * MeV > pt_cuts[2]]
        
        # Apply lepton type and charge cuts
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
    """Process a data processing task from the queue"""
    try:
        # Parse task
        task = json.loads(body.decode())
        sample_type = task['sample_type']
        sample_name = task['sample_name']
        task_id = task['task_id']
        tree = deserialize_data(task['tree'])
        lumi = task.get('lumi', 10)
        fraction = task.get('fraction', 1.0)
        is_mc = task.get('is_mc', False)
        
        print(f"Processing data for {sample_name}")
        start_time = time.time()
        
        # Process the data
        processed_data = process_data(tree, sample_name, is_mc, lumi, None, fraction)
        
        if processed_data is not None:
            # Create an analysis task
            analysis_task = {
                'task_id': task_id,
                'sample_type': sample_type,
                'sample_name': sample_name,
                'data': serialize_data(processed_data),
                'lumi': lumi,
                'fraction': fraction
            }
            
            # Send to analysis queue
            channel = ch.connection.channel()
            channel.queue_declare(queue=ANALYSIS_QUEUE, durable=True)
            channel.basic_publish(
                exchange='',
                routing_key=ANALYSIS_QUEUE,
                body=json.dumps(analysis_task, default=str),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                    content_type='application/json'
                )
            )
        
        elapsed = time.time() - start_time
        print(f"Processed {sample_name} in {round(elapsed, 1)}s")
        
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        print(f"Error processing data for {task['sample_name']}: {e}")
        import traceback
        traceback.print_exc()
        # Acknowledge the message even on error to avoid reprocessing
        ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    """Main function to process data processing tasks from the queue"""
    # Connect to RabbitMQ
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    
    # Declare queue
    channel.queue_declare(queue=PROCESS_QUEUE, durable=True)
    
    # Set prefetch count
    channel.basic_qos(prefetch_count=1)
    
    # Set up consumer
    channel.basic_consume(queue=PROCESS_QUEUE, on_message_callback=callback)
    
    print("Data processor worker started. Waiting for tasks...")
    
    # Start consuming
    channel.start_consuming()

if __name__ == "__main__":
    main()
