import pika
import numpy as np
import matplotlib.pyplot as plt
import json
import time
import logging
import os
import infofile  # Ensure this is available

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
            dsid = infofile.infos[sample]["DSID"]
            file_path = f"{base_path}MC/mc_{dsid}.{sample}.4lep.root"
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

    # Send tasks
    for task in file_paths:
        channel.basic_publish(exchange='', routing_key='task_queue', body=json.dumps(task))
        logger.info(f"Sent task: {task['file_path']} (sample: {task['sample']})")

    logger.info("Sent all tasks to workers")

    # Collect results
    data_masses = []
    mc_masses = {cat: [] for cat in samples if cat != 'data'}
    mc_weights = {cat: [] for cat in samples if cat != 'data'}
    files_processed = 0

    def collect_results(ch, method, properties, body):
        global files_processed
        try:
            result = json.loads(body.decode())
            masses = result["masses"]
            weights = result["weights"]
            is_data = result["is_data"]

            if is_data:
                data_masses.extend(masses)
                logger.info(f"Received {len(masses)} data masses, total: {len(data_masses)}")
            else:
                sample = [task["sample"] for task in file_paths if task["file_path"] == properties.headers.get("file_path", "")][0]
                for cat, info in samples.items():
                    if cat != 'data' and sample in info['list']:
                        mc_masses[cat].extend(masses)
                        mc_weights[cat].extend(weights)
                        logger.info(f"Received {len(masses)} MC masses for {cat}, total: {len(mc_masses[cat])}")
                        break

            ch.basic_ack(delivery_tag=method.delivery_tag)
            files_processed += 1

            if files_processed == len(file_paths):
                # Generate histogram
                xmin = 80  # GeV
                xmax = 250  # GeV
                step_size = 5  # GeV
                bin_edges = np.arange(xmin, xmax + step_size, step_size)
                bin_centres = (bin_edges[:-1] + bin_edges[1:]) / 2

                # Data histogram
                data_x, _ = np.histogram(data_masses, bins=bin_edges)
                data_x_errors = np.sqrt(data_x)

                # MC histograms
                mc_x = []
                mc_w = []
                mc_colors = []
                mc_labels = []
                for cat in samples:
                    if cat != 'data':
                        mc_x.append(np.array(mc_masses[cat]))
                        mc_w.append(np.array(mc_weights[cat]))
                        mc_colors.append(samples[cat]['color'])
                        mc_labels.append(cat)

                # Plot
                plt.figure()
                main_axes = plt.gca()
                main_axes.errorbar(bin_centres, data_x, yerr=data_x_errors, fmt='ko', label='Data')
                mc_heights = main_axes.hist(mc_x, bins=bin_edges, weights=mc_w, stacked=True, 
                                           color=mc_colors, label=mc_labels)
                mc_x_tot = mc_heights[0][-1]  # Total stacked MC
                mc_x_err = np.sqrt(np.histogram(np.concatenate(mc_x), bins=bin_edges, 
                                               weights=np.concatenate(mc_w)**2)[0])
                main_axes.bar(bin_centres, 2 * mc_x_err, bottom=mc_x_tot - mc_x_err, 
                             alpha=0.5, color='none', hatch="////", width=step_size, label='Stat. Unc.')

                main_axes.set_xlim(xmin, xmax)
                main_axes.set_ylim(0, np.max(data_x) * 1.6)
                main_axes.xaxis.set_minor_locator(plt.AutoMinorLocator())
                main_axes.yaxis.set_minor_locator(plt.AutoMinorLocator())
                main_axes.tick_params(which='both', direction='in', top=True, right=True)
                main_axes.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]', fontsize=13, x=1, ha='right')
                main_axes.set_ylabel(f'Events / {step_size} GeV', fontsize=13, y=1, ha='right')
                main_axes.legend(frameon=False)

                # Save and show
                output_path = os.path.join(output_dir, "output_plot.png")
                plt.savefig(output_path)
                logger.info(f"Histogram saved to {output_path}")
                plt.show()
                connection.close()
        except Exception as e:
            logger.error(f"Error processing result: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(queue='result_queue', on_message_callback=collect_results)
    logger.info("Waiting for results...")
    channel.start_consuming()
