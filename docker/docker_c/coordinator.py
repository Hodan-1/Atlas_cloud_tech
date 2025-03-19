import sys
import pika
import json
import time
import logging
import uproot
import awkward as ak


logger = logging.getLogger(__name__)


samples = {
    'data': ['data_A', 'data_B', 'data_C', 'data_D'],
    'mc': ['Zee', 'Zmumu', 'ttbar_lep', 'llll'],
    'signal': ['ggH125_ZZ4lep', 'VBFH125_ZZ4lep', 'WH125_ZZ4lep', 'ZH125_ZZ4lep']
}

def start_tasks():
    """Reads all samples and sends them to RabbitMQ."""
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='data_tasks')
    channel.queue_declare(queue='mc_tasks')
    channel.queue_declare(queue='signal_tasks')
    channel.queue_declare(queue='transverse_tasks')

    for sample in samples['data']:
        channel.basic_publish(exchange='',
            routing_key='data_tasks',
            body=json.dumps({'type': 'data', 'sample': sample}))

    
    for sample in samples['mc']:
        channel.basic_publish(exchange='',
            routing_key='mc_tasks',
            body=json.dumps({'type': 'mc', 'sample': sample}))
        
    for sample in samples['signal']:
        channel.basic_publish(exchange='',
            routing_key='signal_tasks',
            body=json.dumps({'type': 'signal', 'sample': sample}))
        
    connection.close()


if __name__ == "__main__":
    total_tasks = start_tasks()