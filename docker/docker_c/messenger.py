# messenger.py
import pika
import numpy as np
import matplotlib.pyplot as plt
import json
import time
import logging
import os
import infofile

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Output directory
output_dir = "/app/output"
os.makedirs(output_dir, exist_ok=True)

# Base path and samples
base_path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"
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

# Construct file paths
file_paths = []
for category, info in samples.items():
    for sample in info['list']:
        if category == 'data':
            file_path = f"{base_path}Data/{sample}.4lep.root"
        else:
            # Ensure the DSID exists in infofile.infos
            if sample in infofile.infos:
                dsid = infofile.infos[sample]["DSID"]
                file_path = f"{base_path}MC/mc_{dsid}.{sample}.4lep.root"
                logger.info(f"Constructed MC file path: {file_path}")
            else:
                logger.error(f"Sample {sample} not found in infofile.infos")
                continue
        file_paths.append({"file_path": file_path, "sample": sample})

def connect_to_rabbitmq():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
            return connection
        except pika.exceptions.AMQPConnectionError:
            logger.info("Waiting for RabbitMQ...")
            time.sleep(5)

# Initialize RabbitMQ connection
if __name__ == "__main__":
    time.sleep(10)  # Wait for RabbitMQ to initialize
    connection = connect_to_rabbitmq()
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)
    channel.queue_declare(queue='result_queue', durable=True)

    for task in file_paths:
        logger.info(f"Sending task: {task}")
        channel.basic_publish(exchange='', routing_key='task_queue', body=json.dumps(task))
    logger.info("All tasks sent to the queue.")

    # Collect results
    data_masses = []
    mc_masses = {cat: [] for cat in samples if cat != 'data'}
    mc_weights = {cat: [] for cat in samples if cat != 'data'}
    files_processed = 0

    def collect_results(ch, method, properties, body):
        global files_processed
        try:
            result = json.loads(body.decode())
            logger.info(f"Received result: {result.keys()}")
            masses = result["masses"]
            weights = result["weights"]
            is_data = result["is_data"]

            if is_data:
                data_masses.extend(masses)
                logger.info(f"Received {len(masses)} data masses, total: {len(data_masses)}")
            else:
                # Safely get the file_path and sample from headers
                file_path = properties.headers.get("file_path", "") if properties.headers else ""
                sample = properties.headers.get("sample", "") if properties.headers else ""
                if not file_path or not sample:
                    logger.error(f"Missing headers in message: {properties.headers}")
                    return

                for cat, info in samples.items():
                    if cat != 'data' and sample in info['list']:
                        mc_masses[cat].extend(masses)
                        if weights is not None:  # Handle case where weights are None
                            mc_weights[cat].extend(weights)
                        logger.info(f"Received {len(masses)} MC masses for {cat}, total: {len(mc_masses[cat])}")
                        break

            ch.basic_ack(delivery_tag=method.delivery_tag)
            files_processed += 1

            if files_processed == len(file_paths):
                # Generate histogram (same as original code)
                xmin = 80 * GeV
                xmax = 250 * GeV
                step_size = 5 * GeV
                bin_edges = np.arange(start=xmin, stop=xmax + step_size, step=step_size)
                bin_centres = np.arange(start=xmin + step_size / 2, stop=xmax + step_size / 2, step=step_size)

                # Plot data
                data_x, _ = np.histogram(data_masses, bins=bin_edges)
                data_x_errors = np.sqrt(data_x)

                # Plot MC
                mc_x = np.hstack(list(mc_masses.values()))
                mc_weights_all = np.hstack(list(mc_weights.values()))
                mc_x_tot, _ = np.histogram(mc_x, bins=bin_edges, weights=mc_weights_all)
                mc_x_err = np.sqrt(np.histogram(mc_x, bins=bin_edges, weights=mc_weights_all**2)[0])

                # Create plot
                plt.errorbar(bin_centres, data_x, yerr=data_x_errors, fmt='ko', label='Data')
                plt.hist(mc_x, bins=bin_edges, weights=mc_weights_all, stacked=True, color=[samples[cat]['color'] for cat in mc_masses], label=list(mc_masses.keys()))
                plt.bar(bin_centres, 2 * mc_x_err, alpha=0.5, bottom=mc_x_tot - mc_x_err, color='none', hatch="////", width=step_size, label='Stat. Unc.')
                plt.xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]')
                plt.ylabel(f'Events / {step_size} GeV')
                plt.legend()
                plt.savefig(os.path.join(output_dir, "output_plot.png"))
                plt.close()
                logger.info(f"Histogram saved to {os.path.join(output_dir, 'output_plot.png')}")
        except Exception as e:
            logger.error(f"Error processing result: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(queue='result_queue', on_message_callback=collect_results)
    logger.info("Waiting for results...")
    channel.start_consuming()