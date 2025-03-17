import pika
import uproot
import awkward as ak
import vector
import numpy as np
import time
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constant
MeV = 0.001
GeV = 1.0

# Define mass  functions
def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    return (sum_lep_type == 44) | (sum_lep_type == 48) | (sum_lep_type == 52)  

def cut_lep_charge(lep_charge):
    return lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0

def calc_mass(lep_pt, lep_eta, lep_phi, lep_E):
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_E})
    return (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M * MeV


def process_task(file_path):
    logger.info(f"Opening file: {file_path}")
    tree = uproot.open(file_path + ":mini")
    variables = ['lep_pt', 'lep_eta', 'lep_phi', 'lep_E', 'lep_charge', 'lep_type']
    data = tree.arrays(variables, library="ak")

    # Debugging: Log the number of events before cuts
    logger.info(f"Number of events before cuts: {len(data)}")

    # Debugging: Log the first few lepton types and charges
    logger.info(f"First few lepton types: {data['lep_type'][:5]}")
    logger.info(f"First few lepton charges: {data['lep_charge'][:5]}")

    # Apply cuts
    data = data[cut_lep_type(data['lep_type'])]
    data = data[cut_lep_charge(data['lep_charge'])]

    # Debugging: Log the number of events after cuts
    logger.info(f"Number of events after cuts: {len(data)}")

    # Calculate invariant mass
    data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_E'])
    
    # Debugging: Log the first few masses
    logger.info(f"First few masses: {data['mass'][:5]}")

    return data

def callback(ch, method, properties, body):
    file_path = body.decode()
    logger.info(f"Processing {file_path}")

    try:
        #data processed hete
        processed_data = process_task(file_path)

        # Send result back to rabbit
        masses = ak.to_list(processed_data['mass'])
        ch.basic_publish(exchange='', routing_key='result_queue', body= json.dumps(masses))
        logger.info(f"Sent results for {file_path}")

        # Add acknowlegments
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        # reject errors and queue
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def connect_to_rabbitmq():
## failing to connect so added a logic that retries:
    while True:
        try:
            # Connect to RabbitMQ
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            return connection
    
        except pika.exceptions.AMQPConnectionError:
            print("Waiting for RabbitMQ...")
            time.sleep(5)

# Wait for RabbitMQ to initialize
time.sleep(10)

#Connect to RabbitMQ
connection = connect_to_rabbitmq()
channel = connection.channel()

# Declare queues
channel.queue_declare(queue='task_queue', durable=True)
channel.queue_declare(queue='result_queue', durable=True)

# Start initiate? tasks
logger.info("Starting to consume messages...")
channel.basic_consume(queue='task_queue', on_message_callback=callback)
channel.start_consuming()
