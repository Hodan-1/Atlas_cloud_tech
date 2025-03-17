import pika
import numpy as np
import matplotlib.pyplot as plt
import json
import time
import logging

##Added the needed files
file_paths = [
    "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/Data/data_A.4lep.root",
    "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/Data/data_B.4lep.root",
    "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/Data/data_C.4lep.root",
    "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/Data/data_D.4lep.root",
    
]

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to RabbitMQ with retry logic
def connect_to_rabbitmq():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            return connection
        except pika.exceptions.AMQPConnectionError:
            logger.info("Waiting for RabbitMQ")
            time.sleep(5)

# Wait for RabbitMQ to initialize
time.sleep(10)

#CONNNECT TO RABBITMQ
connection = connect_to_rabbitmq()
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)
channel.queue_declare(queue='result_queue', durable=True)


# Send tasks to the queue
for file in file_paths:
    channel.basic_publish(exchange='', routing_key='task_queue', body= file)
 
logger.info(" [x] Sent tasks to workers")

# Collect results from workers. combined agregator to this...
results = []
def collect_results(ch, method, properties, body):
    results.extend(eval(body.decode()))
    if len(results) == len(file_paths):
        # Generate the final histogram
        bin_edges = np.arange(80, 250 + 5, 5)
        plt.hist(results, bins=bin_edges, edgecolor='black')
        plt.xlabel('4-lepton invariant mass [GeV]')
        plt.ylabel('Events / 5 GeV')
        plt.title('Final Histogram')
        plt.show()
        connection.close()

channel.basic_consume(queue='result_queue', on_message_callback=collect_results)

logger.info(' Waiting for results.')
channel.start_consuming()

