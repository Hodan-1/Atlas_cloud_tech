import os
import sys
import time
import json
import pika
import numpy as np
import awkward as ak
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import matplotlib
matplotlib.use('Agg')  

# Add the common directory to the Python path to access shared modules
sys.path.append('/app')
from connect import connect_to_rabbitmq, deserialize_awkward
from constants import SAMPLES, RESULT_QUEUE, VISUALIZATION_QUEUE, setup_histogram_bins, GeV

# Configure logging to output to the console with a basic format
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def calculate_histogram_data(data, bin_edges):
    """
    Calculate histogram data and errors for the given data and bin edges.
    
    Args:
        data: The input data containing 'mass' field.
        bin_edges: The edges of the histogram bins.
    
    Returns:
        tuple: A tuple containing the histogram data and its errors.
    """
    # Calculate histogram data and errors
    data_x, _ = np.histogram(ak.to_numpy(data['mass']), bins=bin_edges)
    data_x_errors = np.sqrt(data_x)
    logging.debug("Histogram data and errors calculated successfully.")
    return data_x, data_x_errors

def prepare_plot_data(all_data, samples, bin_edges):
    """
    Prepare data for plotting by organiing it into a structured format.
    
    Args:
        all_data: Dictionary containing all processed data.
        samples: Dictionary containing sample information.
        bin_edges: The edges of the histogram bins.
    
    Returns:
        dict: A dictionary containing data organized for plotting.
    """
    # Extract data for plotting
    data_x, data_x_errors = calculate_histogram_data(all_data['data'], bin_edges)
    
    # Extract signal data
    signal_x = ak.to_numpy(all_data[r'Signal ($m_H$ = 125 GeV)']['mass'])
    signal_weights = ak.to_numpy(all_data[r'Signal ($m_H$ = 125 GeV)'].totalWeight)
    signal_color = samples[r'Signal ($m_H$ = 125 GeV)']['color']
    
    # Extract background MC data
    mc_x = []
    mc_weights = []
    mc_colors = []
    mc_labels = []
    
    for s in samples:
        if s not in ['data', r'Signal ($m_H$ = 125 GeV)']:
            mc_x.append(ak.to_numpy(all_data[s]['mass']))
            mc_weights.append(ak.to_numpy(all_data[s].totalWeight))
            mc_colors.append(samples[s]['color'])
            mc_labels.append(s)
    
    # Convert numpy arrays to lists for JSON serialization
    plot_data = {
        'data_x': data_x.tolist(),
        'data_x_errors': data_x_errors.tolist(),
        'signal_x': signal_x.tolist(),
        'signal_weights': signal_weights.tolist(),
        'signal_color': signal_color,
        'mc_x': [x.tolist() for x in mc_x],
        'mc_weights': [w.tolist() for w in mc_weights],
        'mc_colors': mc_colors,
        'mc_labels': mc_labels
    }
    
    logging.debug("Plot data prepared successfully.")
    return plot_data

def main():
    """
    Main function to collect results from RabbitMQ, perform analysis, and send results for visualization.
    """
    # Connect to RabbitMQ
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    
    # Declare queues for results and visualization
    channel.queue_declare(queue=RESULT_QUEUE, durable=True)
    channel.queue_declare(queue=VISUALIZATION_QUEUE, durable=True)
    
    # Set up histogram bins for analysis
    bin_edges, bin_centres = setup_histogram_bins()
    
    # Dictionary to hold all processed data
    all_data = {}
    expected_samples = sum(len(sample_info['list']) for sample_info in SAMPLES.values())
    received_samples = 0
    
    # Get analysis parameters from environment variables
    lumi = float(os.environ.get('LUMI', '10'))
    fraction = float(os.environ.get('FRACTION', '1.0'))
    
    logging.info(f"Analysis worker started. Waiting for {expected_samples} sample results...")
    
    # Process results until all samples are received
    while received_samples < expected_samples:
        # Get a message from the result queue
        method_frame, header_frame, body = channel.basic_get(queue=RESULT_QUEUE)
        
        if method_frame:
            # Parse the result from the message body
            result = json.loads(body.decode())
            
            # Skip processing if there was an error in the result
            if result['error']:
                logging.error(f"Error processing {result['sample_type']} - {result['sample_name']}: {result['error']}")
                channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                received_samples += 1
                continue
            
            # Deserialize the awkward array data
            data = deserialize_awkward(result['data'])
            
            # Add data to the all_data dictionary
            if result['sample_type'] not in all_data:
                all_data[result['sample_type']] = data
            else:
                all_data[result['sample_type']] = ak.concatenate([all_data[result['sample_type']], data])
            
            logging.info(f"Received result for {result['sample_type']} - {result['sample_name']}")
            
            # Acknowledge the message to remove it from the queue
            channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            
            received_samples += 1
        else:
            # No message available, wait a bit before retrying
            time.sleep(1)
    
    logging.info(f"Received all {received_samples} sample results. Performing analysis...")
    
    # Prepare data for plotting
    plot_data = prepare_plot_data(all_data, SAMPLES, bin_edges)
    
    # Create analysis task for visualization
    analysis_task = {
        'plot_data': plot_data,
        'bin_edges': bin_edges.tolist(),
        'bin_centres': bin_centres.tolist(),
        'lumi': lumi,
        'fraction': fraction
    }
    
    # Send analysis task to the visualization queue
    properties = pika.BasicProperties(
        delivery_mode=2,  # Make the message persistent
    )
    
    channel.basic_publish(
        exchange='',
        routing_key=VISUALIZATION_QUEUE,
        body=json.dumps(analysis_task),
        properties=properties
    )
    
    logging.info("Analysis completed and sent to visualization worker.")
    
    # Close the RabbitMQ connection
    connection.close()

if __name__ == "__main__":
    main()