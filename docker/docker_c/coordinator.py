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

# Output directory
output_dir = "/app/output"
os.makedirs(output_dir, exist_ok=True)

# File paths for data and MC
file_paths = [
    {"file_path": "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/Data/data_A.4lep.root", "is_mc": False},
    {"file_path": "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/Data/data_B.4lep.root", "is_mc": False},
    {"file_path": "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/Data/data_C.4lep.root", "is_mc": False},
    {"file_path": "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/Data/data_D.4lep.root", "is_mc": False},
    {"file_path": "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/MC/mc_361106.Zee.4lep.root", "is_mc": True},
]

# Connect to RabbitMQ with retry logic
def connect_to_rabbitmq():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            return connection
        except pika.exceptions.AMQPConnectionError:
            logger.info("Waiting for RabbitMQ...")
            time.sleep(5)

# Wait for RabbitMQ to initialize
time.sleep(10)

# Connect to RabbitMQ
connection = connect_to_rabbitmq()
channel = connection.channel()

# Declare queues
channel.queue_declare(queue='task_queue', durable=True)
channel.queue_declare(queue='result_queue', durable=True)

# Send tasks to the queue
for task in file_paths:
    channel.basic_publish(exchange='', routing_key='task_queue', body=json.dumps(task))
    logger.info(f"Sent task: {task['file_path']}")

logger.info(" [x] Sent tasks to workers")

# Collect results from workers
results = []
mc_results = []
files_processed = 0

def collect_results(ch, method, properties, body):
    global files_processed
    result = json.loads(body.decode())
    if method.headers.get('is_mc', False):
        mc_results.extend(result)
    else:
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

        # Create histogram for data
        data_x, _ = np.histogram(results, bins=bin_edges)
        bin_centres = (bin_edges[:-1] + bin_edges[1:]) / 2  # Calculate bin centers
        data_x_errors = np.sqrt(data_x)  # Statistical errors

        # Create histogram for MC
        mc_x, _ = np.histogram(mc_results, bins=bin_edges, weights=[w['totalWeight'] for w in mc_results])
        mc_x_err = np.sqrt(np.histogram(mc_results, bins=bin_edges, weights=[w['totalWeight']**2 for w in mc_results])[0])

        # Plot
        plt.figure()
        plt.errorbar(bin_centres, data_x, yerr=data_x_errors, fmt='ko', label='Data')
        plt.hist(bin_centres, bins=bin_edges, weights=mc_x, color='purple', label='MC Background', alpha=0.7)
        plt.bar(bin_centres, 2 * mc_x_err, bottom=mc_x - mc_x_err, color='none', edgecolor='black', hatch='////', label='MC Stat. Unc.')
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

# Start consuming results
channel.basic_consume(queue='result_queue', on_message_callback=collect_results)

logger.info('Waiting for results...')
channel.start_consuming()
