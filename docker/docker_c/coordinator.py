import sys
import json
import time
from config import BASE_PATH, RABBITMQ_HOST
from utils import connect_to_rabbitmq, declare_queue
import logging
from data_reader import samples, get_file_path  # Import from data_reader

logger = logging.getLogger(__name__)

def send_data(sample_type, sample_name, channel):
    """Reads ROOT file and sends JSON data to RabbitMQ."""
    file_path = get_file_path(sample_name, sample_type)  # Use imported function
    try:
        tree = uproot.open(file_path + ":mini")
        variables = ["lep_pt", "lep_eta", "lep_phi", "lep_E", "lep_charge", "lep_type"]

        # Add MC weight variables for MC samples
        if sample_type != "data":
            variables += ["mcWeight", "scaleFactor_PILEUP", "scaleFactor_ELE", "scaleFactor_MUON", "scaleFactor_LepTRIGGER"]

        data = tree.arrays(variables, library="ak")

        # Convert data to JSON
        json_data = ak.to_json(data)

        # Send data to RabbitMQ queue
        channel.basic_publish(exchange="", routing_key="data_queue", body=json_data)
        logger.info(f"Sent event data for {sample_type} - {sample_name}")

    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")

def start_tasks():
    """Reads all samples and sends them to RabbitMQ."""
    connection, channel = connect_to_rabbitmq(RABBITMQ_HOST)
    declare_queue(channel, "data_queue")

    total_tasks = 0

    for sample_type, details in samples.items():  # Use imported samples dictionary
        for sample_name in details["list"]:
            logger.info(f"Sending task: {sample_type} - {sample_name}")
            send_data(sample_type, sample_name, channel)
            total_tasks += 1

    connection.close()
    logger.info(f"All {total_tasks} tasks sent to data_queue.")
    return total_tasks

if __name__ == "__main__":
    total_tasks = start_tasks()