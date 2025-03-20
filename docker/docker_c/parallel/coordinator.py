import sys
import pika
import json
import time
import logging
import uproot
import awkward as ak
from config import samples

logger = logging.getLogger(__name__)


def start_tasks():
    """Reads all samples and sends them to RabbitMQ."""
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='data_tasks')
    channel.queue_declare(queue='mc_background_tasks')
    channel.queue_declare(queue='mc_signal_tasks')
    channel.queue_declare(queue='transverse_tasks')

    for sample in samples['data']['list']:
        channel.basic_publish(exchange='',
            routing_key='data_tasks',
            body=json.dumps({'sample': sample, 'type': 'data'}))


    # MC Background tasks
    background_keys = [r'Background $Z,t\bar{t}$', r'Background $ZZ^*$']
    for bg_key in background_keys:
        for sample in samples[bg_key]['list']:
            channel.basic_publish(exchange='', routing_key='mc_background_tasks', body=json.dumps({'sample': sample, 'type': 'background'}))

    # Signal tasks
    signal_key = r'Signal ($m_H$ = 125 GeV)'
    for sample in samples[signal_key]['list']:
        channel.basic_publish(exchange='', routing_key='mc_signal_tasks', body=json.dumps({'sample': sample, 'type': 'signal'}))

    channel.basic_publish(exchange='',
        routing_key='transverse_tasks',
        body=json.dumps({'action': 'process'}))
        
    connection.close()


if __name__ == "__main__":
    total_tasks = start_tasks()