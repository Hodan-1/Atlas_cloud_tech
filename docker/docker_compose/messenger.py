import pika
import numpy as np
import matplotlib.pyplot as plt
import json
import time
import logging
import os

# Assuming infofile.py exists for sample metadata
try:
    import infofile
except ImportError:
    logging.error("infofile module not found. MC samples may fail.")
    infofile = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Output directory
output_dir = "/app/output"
os.makedirs(output_dir, exist_ok=True)

# Sample definitions
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
            if not infofile or sample not in infofile.infos:
                logger.warning(f"Skipping {sample}: not in infofile")
                continue
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

if __name__ == "__main__":
    time.sleep(10)
    connection = connect_to_rabbitmq()
    channel = connection.channel()

    channel.queue_declare(queue='task_queue', durable=True)
    channel.queue_declare(queue='result_queue', durable=True)

    for task in file_paths:
        logger.info(f"Sending task: {task}")
        channel.basic_publish(exchange='', routing_key='task_queue', body=json.dumps(task))
    logger.info(f"Sent {len(file_paths)} tasks to queue")

    # Collect results
    data_masses = []
    mc_masses = {cat: [] for cat in samples if cat != 'data'}
    mc_weights = {cat: [] for cat in samples if cat != 'data'}
    files_processed = 0

    def collect_results(ch, method, properties, body):
        nonlocal files_processed
        try:
            result = json.loads(body.decode())
            masses = result["masses"]
            weights = result["weights"]
            is_data = result["is_data"]
            headers = properties.headers or {}
            sample = headers.get("sample", "unknown")

            if is_data:
                data_masses.extend(masses)
                logger.info(f"Received {len(masses)} data masses from {sample}, total: {len(data_masses)}")
            else:
                for cat, info in samples.items():
                    if cat != 'data' and sample in info['list']:
                        mc_masses[cat].extend(masses)
                        mc_weights[cat].extend(weights or [1] * len(masses))
                        logger.info(f"Received {len(masses)} MC masses for {cat} from {sample}, total: {len(mc_masses[cat])}")
                        break
                else:
                    logger.warning(f"Sample {sample} not categorized")

            ch.basic_ack(delivery_tag=method.delivery_tag)
            files_processed += 1

            if files_processed == len(file_paths):
                # Generate histogram
                xmin, xmax, step_size = 80, 250, 5
                bin_edges = np.arange(xmin, xmax + step_size, step_size)
                bin_centres = (bin_edges[:-1] + bin_edges[1:]) / 2

                # Data histogram
                data_hist, _ = np.histogram(data_masses, bins=bin_edges)
                data_errors = np.sqrt(data_hist)

                # MC histograms
                mc_hists = []
                mc_colors = []
                for cat in mc_masses:
                    hist, _ = np.histogram(mc_masses[cat], bins=bin_edges, weights=mc_weights[cat])
                    mc_hists.append(hist)
                    mc_colors.append(samples[cat]['color'])
                mc_tot = np.sum(mc_hists, axis=0)
                mc_err = np.sqrt(np.sum([h**2 for h in mc_hists], axis=0))

                # Plot
                plt.figure(figsize=(10, 6))
                plt.errorbar(bin_centres, data_hist, yerr=data_errors, fmt='ko', label='Data')
                plt.hist([np.array(mc_masses[cat]) for cat in mc_masses], bins=bin_edges, weights=[np.array(mc_weights[cat]) for cat in mc_weights], stacked=True, color=mc_colors, label=list(mc_masses.keys()))
                plt.bar(bin_centres, 2 * mc_err, bottom=mc_tot - mc_err, alpha=0.5, color='none', hatch="////", width=step_size, label='Stat. Unc.')
                plt.xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]')
                plt.ylabel(f'Events / {step_size} GeV')
                plt.legend()
                plt.xlim(xmin, xmax)
                plt.ylim(0, max(np.max(data_hist) * 1.5, np.max(mc_tot) * 1.5))

                output_path = os.path.join(output_dir, "output_plot.png")
                plt.savefig(output_path)
                logger.info(f"Histogram saved to {output_path}")
                plt.close()
                connection.close()
        except Exception as e:
            logger.error(f"Error processing result: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(queue='result_queue', on_message_callback=collect_results)
    logger.info("Waiting for results...")
    channel.start_consuming()