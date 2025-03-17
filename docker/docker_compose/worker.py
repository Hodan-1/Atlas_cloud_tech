import pika
import uproot
import awkward as ak
import vector
import numpy as np

# Constant
MeV = 0.001
GeV = 1.0


# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue='tasks')
channel.queue_declare(queue='results')


import pika
import uproot
import awkward as ak
import vector
import numpy as np

# Constants
MeV = 0.001
GeV = 1.0

# Define mass  functions
def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    return (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)

def cut_lep_charge(lep_charge):
    return lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0

def calc_mass(lep_pt, lep_eta, lep_phi, lep_E):
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_E})
    return (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M * MeV


def process_task(file_path):
    #Full data set
    tree = uproot.open(file_path + ":mini")
    variables = ['lep_pt', 'lep_eta', 'lep_phi', 'lep_E', 'lep_charge', 'lep_type']
    data = tree.arrays(variables, library="ak")

# Apply cuts
    data = data[~cut_lep_type(data['lep_type'])]
    data = data[~cut_lep_charge(data['lep_charge'])]
    
    # Calculate invariant mass
    data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_E'])
    
    return data

def callback(ch, method, properties, body):
    file_path = body.decode()
    #debug ... provides updates to process
    print(f"Processing {file_path}")

    #data processed hete
    processed_data = process_task(file_path)

    # Send result back to rabbit
    channel.basic_publish(exchange='', routing_key='result_queue', body=str(ak.to_list(processed_data['mass'])))
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declare queues
channel.queue_declare(queue='task_queue', durable=True)
channel.queue_declare(queue='result_queue', durable=True)


channel.basic_consume(queue='task_queue', on_message_callback=callback)
print('Worker waiting for messages...')
channel.start_consuming()
