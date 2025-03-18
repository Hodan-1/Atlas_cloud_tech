import json
import numpy as np
import matplotlib.pyplot as plt
from config import RABBITMQ_HOST
from utils import connect_to_rabbitmq, declare_queue
import logging

logger = logging.getLogger(__name__)

def aggregate_summaries(expected_tasks=10):
    summaries = []
    connection, channel = connect_to_rabbitmq(RABBITMQ_HOST)
    declare_queue(channel, "summary_queue")

    def callback(ch, method, properties, body):
        summaries.append(json.loads(body))
        if len(summaries) == expected_tasks:
            generate_graph(summaries)
            ch.stop_consuming()

    channel.basic_consume(queue="summary_queue", on_message_callback=callback, auto_ack=True)
    logger.info("Aggregator is waiting for summaries...")
    channel.start_consuming()

import numpy as np
import matplotlib.pyplot as plt

def calculate_signal_significance(masses, weights, signal_masses, signal_weights):
    """Calculate signal significance using event counts in signal/background regions."""
    signal_region = (masses >= 120) & (masses <= 130)  # Around Higgs mass
    background_region = (masses >= 110) & (masses <= 140)  # Wider background region

    N_sig = np.sum(signal_weights[signal_region])
    N_bg = np.sum(weights[background_region])

    if N_bg == 0:
        return 0  # Avoid division by zero

    signal_significance = N_sig / np.sqrt(N_bg + 0.3 * N_bg**2)
    return signal_significance

def generate_graph(summaries):
    masses = []
    weights = []

    for summary in summaries:
        masses.extend(summary["masses"])
        weights.extend(summary["weights"])

    # Define binning (same as original script)
    xmin = 80  # GeV
    xmax = 250  # GeV
    step_size = 5  # GeV
    bin_edges = np.arange(xmin, xmax + step_size, step_size)
    bin_centres = np.arange(xmin + step_size / 2, xmax + step_size / 2, step_size)

    # Plot data
    plt.hist(masses, bins=bin_edges, weights=weights, alpha=0.7, label="Invariant Mass")
    plt.xlabel("Invariant Mass [GeV]")
    plt.ylabel("Events / 5 GeV")
    plt.legend()
    plt.savefig("/app/output/final_graph.png")
    
    logger.info("📈 Final graph saved to /app/output/final_graph.png")

if __name__ == "__main__":
    aggregate_summaries()

