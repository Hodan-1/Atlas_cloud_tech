import json
import logging
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

def send_tasks():
    """Sends tasks to RabbitMQ."""
    connection, channel = connect_to_rabbitmq(RABBITMQ_HOST)
    declare_queue(channel, "data_queue")

    for category, info in samples.items():
        for sample in info['list']:
            file_path = get_file_path(sample, category)
            if file_path:
                message = {"file_path": file_path, "sample": sample, "category": category}
                json_message = json.dumps(message)
                logger.info(f"Sending message: {json_message}")
                channel.basic_publish(exchange="", routing_key="data_queue", body=json_message)

    connection.close()

if __name__ == "__main__":
    send_tasks()


