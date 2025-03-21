import pika
import numpy as np
import json
import time
import logging
import os
import syst
import infofile

def check_file_exists(file_path):
    """Check if a file exists by trying to open it"""
    try:
        with uproot.open(file_path) as f:
            return True
    except:
        return False
            

def main():
    connection = connect_to_rabbitmq()
    channel = connection.channel()

    channel.queue_declare(queue=TASK_QUEUE, durable=True)

    # Set message persistence
    properties = pika.BasicProperties(
        delivery_mode=2,  
    )

    # Get parameters from environment variables
    lumi = float(os.environ.get('LUMI', '10'))
    fraction = float(os.environ.get('FRACTION', '1.0'))
    pt_cuts_str = os.environ.get('PT_CUTS', '')
    pt_cuts = [float(x) for x in pt_cuts_str.split(',')] if pt_cuts_str else None
    
    print(f"Starting data loader with lumi={lumi}, fraction={fraction}, pt_cuts={pt_cuts}")
    
        # Construct file paths
    task_count = 0
    for sample_type, sample_info in SAMPLES.items():
        for sample_name in sample_info['list']:
            if sample_type == 'data':
                prefix = "Data/"
                file_path = PATH + prefix + sample_name + ".4lep.root"
            else:
                prefix = "MC/mc_" + str(infofile.infos[sample_name]["DSID"]) + "."
                file_path = f"{base_path}MC/mc_{dsid}.{sample}.4lep.root"
                file_paths = PATH + prefix + sample_name + ".4lep.root"
            
            # Skip if file doesn't exist
            if not check_file_exists(file_path):
                print(f"File not found: {file_path}")
                continue

            # Create task
            task = {
                'sample_type': sample_type,
                'sample_name': sample_name,
                'lumi': lumi,
                'fraction': fraction,
                'pt_cuts': pt_cuts
            }
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
