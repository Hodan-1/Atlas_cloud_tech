import pika
import numpy as np
import matplotlib.pyplot as plt
import json
import time
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


## want histograsm to print in a seperate directory and pop up
# Output directory
output_dir = "/app/output"
os.makedirs(output_dir, exist_ok=True)

##Added the needed files
file_paths = [
    "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/Data/data_A.4lep.root",
    "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/Data/data_B.4lep.root",
    "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/Data/data_C.4lep.root",
    "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/Data/data_D.4lep.root",
    
]



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
    logger.info("Sent tasks: {file_path}")

logger.info(" [x] Sent tasks to workers")

# Collect results from workers. combined agregator to this...
results = []
files_processed = 0

def collect_results(ch, method, properties, body):
    global files_processed
    result = json.loads(body.decode())
    results.extend(result)
    logger.info(f"Received {len(result)} results")

    # Acknowledge the message
    ch.basic_ack(delivery_tag=method.delivery_tag)
    files_processed += 1

    # Check if all files have been processed
    if files_processed == len(file_paths):
        # Generate the final histogram
        xmin = 80  # GeV
        xmax = 250  # GeV
        step_size = 5  # GeV
        bin_edges = np.arange(xmin, xmax + step_size, step_size)

        # Create histogram
        data_x, bin_edges = np.histogram(results, bins=bin_edges)
        bin_centres = (bin_edges[:-1] + bin_edges[1:]) / 2  # Calculate bin centers
        data_x_errors = np.sqrt(data_x)  # Statistical errors

        # Plot
        plt.figure()
        plt.errorbar(bin_centres, data_x, yerr=data_x_errors, fmt='ko', label='Statistical Errors')
        plt.xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]', fontsize=13)
        plt.ylabel(f'Events / {step_size} GeV', fontsize=13)
        plt.title('Final Histogram')
        plt.xlim(xmin, xmax)
        plt.ylim(0, np.amax(data_x) * 1.6)
        plt.minorticks_on()
        plt.legend(frameon=False)

        # Save the histogram
        output_path = os.path.join(output_dir, "output_plot.png")
        plt.savefig(output_path)
        logger.info(f"Histogram saved to {output_path}")

        plt.show()
        connection.close()


channel.basic_consume(queue='result_queue', on_message_callback=collect_results)

logger.info(' Waiting for results.')
channel.start_consuming()

