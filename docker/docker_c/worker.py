import pika
import uproot
import awkward as ak
import vector
import numpy as np
import time
import logging
import json
import infofile  # Ensure this is available

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
MeV = 0.001
GeV = 1.0
LUMI = 10  # fb^-1, matching original code's combined data_A-D

# Define functions
def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    return (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)

def cut_lep_charge(lep_charge):
    return lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0

def calc_mass(lep_pt, lep_eta, lep_phi, lep_E):
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_E})
    return (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M * MeV

def calc_weight(sample, events):
    if sample.startswith("data_"):
        return np.ones(len(events))  # No weights for data
    info = infofile.infos[sample]
    xsec_weight = (LUMI * 1000 * info["xsec"]) / (info["sumw"] * info["red_eff"])  # fb^-1 to pb^-1
    weight_vars = ["mcWeight", "scaleFactor_PILEUP", "scaleFactor_ELE", "scaleFactor_MUON", "scaleFactor_LepTRIGGER"]
    total_weight = xsec_weight
    for var in weight_vars:
        total_weight = total_weight * events[var]
    return total_weight

def process_task(file_path, sample):
    logger.info(f"Opening file: {file_path} (sample: {sample})")
    try:
        tree = uproot.open(file_path + ":mini")
        base_vars = ['lep_pt', 'lep_eta', 'lep_phi', 'lep_E', 'lep_charge', 'lep_type']
        weight_vars = ["mcWeight", "scaleFactor_PILEUP", "scaleFactor_ELE", "scaleFactor_MUON", "scaleFactor_LepTRIGGER"]
        variables = base_vars if sample.startswith("data_") else base_vars + weight_vars
        data = tree.arrays(variables, library="ak")

        logger.info(f"Number of events before cuts: {len(data)}")
        data = data[cut_lep_type(data['lep_type'])]
        data = data[cut_lep_charge(data['lep_charge'])]
        logger.info(f"Number of events after cuts: {len(data)}")

        masses = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_E'])
        weights = calc_weight(sample, data) if not sample.startswith("data_") else None
        logger.info(f"First few masses: {masses[:5]}")
        if weights is not None:
            logger.info(f"First few weights: {weights[:5]}")

        result = {
            "masses": ak.to_list(masses),
            "weights": ak.to_list(weights) if weights is not None else None,
            "is_data": sample.startswith("data_")
        }
        return result
    except Exception as e:
        logger.error(f"Failed to process {file_path}: {e}")
        raise

def callback(ch, method, properties, body):
    try:
        message = json.loads(body.decode())
        file_path = message["file_path"]
        sample = message["sample"]
        logger.info(f"Processing {file_path} (sample: {sample})")
        result = process_task(file_path, sample)
        ch.basic_publish(exchange='', routing_key='result_queue', body=json.dumps(result))
        logger.info(f"Sent results for {file_path}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def connect_to_rabbitmq():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            return connection
        except pika.exceptions.AMQPConnectionError:
            logger.info("Waiting for RabbitMQ...")
            time.sleep(5)

# Main execution
if __name__ == "__main__":
    time.sleep(10)  # Wait for RabbitMQ to initialize
    connection = connect_to_rabbitmq()
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)
    channel.queue_declare(queue='result_queue', durable=True)

    logger.info("Starting to consume messages...")
    channel.basic_consume(queue='task_queue', on_message_callback=callback)
    channel.start_consuming()
