import json
import logging
import uuid
import uproot
import awkward as ak
from config import BASE_PATH, RABBITMQ_HOST
from utils import connect_to_rabbitmq, declare_queue
import infofile

logger = logging.getLogger(__name__)


samples = {
    'data': {
        'list': ['data_A', 'data_B', 'data_C', 'data_D'],
    },
    'Background $Z,t\\bar{t}$': {
        'list': ['Zee', 'Zmumu', 'ttbar_lep'],
        'color': "#6b59d3"
    },
    'Background $ZZ^*$': {
        'list': ['llll'],
        'color': "#ff0000"
    },
    'Signal ($m_H$ = 125 GeV)': {
        'list': ['ggH125_ZZ4lep', 'VBFH125_ZZ4lep', 'WH125_ZZ4lep', 'ZH125_ZZ4lep'],
        'color': "#00cdff"
    },
}

def get_file_path(sample, category):
    """Returns the correct file path for a given sample."""
    if category == 'data':
        return f"{BASE_PATH}Data/{sample}.4lep.root"
    
    # Get DSID for MC samples
    if sample in infofile.infos:
        dsid = infofile.infos[sample]["DSID"]
        return f"{BASE_PATH}MC/mc_{dsid}.{sample}.4lep.root"
    
    logger.error(f"Sample {sample} not found in infofile.infos")
    return None



CHUNK_SIZE = 1000  # Number of events per message (Adjust as needed)


def send_data_in_chunks(channel, routing_key, data, chunk_size=10000):
    """Sends data to RabbitMQ in smaller chunks at the event level."""
    
    # Generate a unique ID for tracking
    message_id = str(uuid.uuid4())

    # Split the data **before** converting it to JSON
    total_events = len(data["data"]["lep_pt"])  
    total_chunks = (total_events + chunk_size - 1) // chunk_size  # Compute chunk count

    for i in range(total_chunks):
        chunk_data = {
            "message_id": message_id,
            "chunk_index": i,
            "total_chunks": total_chunks,
            "file_path": data["file_path"],
            "sample": data["sample"],
            "category": data["category"],
            "data": {  
                key: value[i * chunk_size:(i + 1) * chunk_size]  
                for key, value in data["data"].items()
            },
        }

        json_message = json.dumps(chunk_data)
        channel.basic_publish(exchange="", routing_key=routing_key, body=json_message)
    
    logger.info(f"Sent {total_chunks} chunks for {data['sample']}")

def process_and_send_tasks():
    """Reads data from ROOT files and sends it to RabbitMQ in chunks."""
    connection, channel = connect_to_rabbitmq(RABBITMQ_HOST)
    declare_queue(channel, "data_queue")

    for category, info in samples.items():
        for sample in info['list']:
            file_path = get_file_path(sample, category)
            if file_path:
                try:
                    tree = uproot.open(file_path + ":mini")
                    variables = ["lep_pt", "lep_eta", "lep_phi", "lep_E", "lep_charge", "lep_type"]
                    data = tree.arrays(variables, library="ak")

                    message_data = {
                        "file_path": file_path,
                        "sample": sample,
                        "category": category,
                        "data": ak.to_list(data)  # Convert Awkward Array to list
                    }

                    send_data_in_chunks(channel, "data_queue", message_data)
                
                except Exception as e:
                    logger.error(f" Error reading {file_path}: {e}")

    connection.close()

if __name__ == "__main__":
    process_and_send_tasks()
