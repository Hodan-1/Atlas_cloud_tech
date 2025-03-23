import os
import sys
import time
import pika
import json
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import matplotlib
matplotlib.use('Agg')  # Use a non-interactive backend for matplotlib

# Add the common directory to the Python path to access shared modules
sys.path.append('/app')
from connect import connect_to_rabbitmq
from constants import VISUALIZATION_QUEUE, GeV

# Configure logging to output to the console with a basic format
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def plot_mass_histogram(plot_data, bin_edges, bin_centres, step_size=5, lumi=10, fraction=1.0):
    """
    Plot a mass histogram and save it to a file.
    
    Args:
        plot_data (dict): Dictionary containing data for plotting.
        bin_edges (np.array): Edges of the histogram bins.
        bin_centres (np.array): Centres of the histogram bins.
        step_size (int): Step size for the histogram bins (default: 5).
        lumi (float): Integrated luminosity in fb^-1 (default: 10).
        fraction (float): Fraction of the data to use (default: 1.0).
    
    Returns:
        dict: A dictionary containing the signal count, background count, signal significance, and plot path.
    """
    # Convert lists back to numpy arrays
    data_x = np.array(plot_data['data_x'])
    data_x_errors = np.array(plot_data['data_x_errors'])
    signal_x = np.array(plot_data['signal_x'])
    signal_weights = np.array(plot_data['signal_weights'])
    signal_color = plot_data['signal_color']
    mc_x = [np.array(x) for x in plot_data['mc_x']]
    mc_weights = [np.array(w) for w in plot_data['mc_weights']]
    mc_colors = plot_data['mc_colors']
    mc_labels = plot_data['mc_labels']
    
    # Create figure
    plt.figure(figsize=(10, 8))
    main_axes = plt.gca()
    
    # Plot the data points
    main_axes.errorbar(x=bin_centres, y=data_x, yerr=data_x_errors,
                       fmt='ko',  # 'k' means black and 'o' is for circles
                       label='Data')
    
    # Plot the Monte Carlo bars
    mc_heights = main_axes.hist(mc_x, bins=bin_edges,
                                weights=mc_weights, stacked=True,
                                color=mc_colors, label=mc_labels)
    
    mc_x_tot = mc_heights[0][-1]  # Stacked background MC y-axis value
    
    # Calculate MC statistical uncertainty: sqrt(sum w^2)
    mc_x_err = np.sqrt(np.histogram(np.hstack(mc_x), bins=bin_edges, weights=np.hstack(mc_weights)**2)[0])
    
    # Plot the statistical uncertainty
    main_axes.bar(bin_centres,  # x
                  2 * mc_x_err,  # heights
                  alpha=0.5,  # half transparency
                  bottom=mc_x_tot - mc_x_err, color='none',
                  hatch="////", width=step_size, label='Stat. Unc.')
    
    # Plot the signal bar
    signal_heights = main_axes.hist(signal_x, bins=bin_edges, bottom=mc_x_tot,
                                    weights=signal_weights, color=signal_color,
                                    label=r'Signal ($m_H$ = 125 GeV)')
    
    # Set the x-limit of the main axes
    main_axes.set_xlim(left=bin_edges[0], right=bin_edges[-1])
    
    # Separation of x-axis minor ticks
    main_axes.xaxis.set_minor_locator(AutoMinorLocator())
    
    # Set the axis tick parameters for the main axes
    main_axes.tick_params(which='both',  # ticks on both x and y axes
                          direction='in',  # Put ticks inside and outside the axes
                          top=True,  # draw ticks on the top axis
                          right=True)  # draw ticks on the right axis
    
    # X-axis label
    main_axes.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]',
                         fontsize=13, x=1, horizontalalignment='right')
    
    # Write y-axis label for main axes
    main_axes.set_ylabel('Events / ' + str(step_size) + ' GeV',
                         y=1, horizontalalignment='right')
    
    # Set y-axis limits for main axes
    main_axes.set_ylim(bottom=0, top=np.amax(data_x) * 1.6)
    
    # Add minor ticks on y-axis for main axes
    main_axes.yaxis.set_minor_locator(AutoMinorLocator())
    
    # Add text 'ATLAS Open Data' on plot
    plt.text(0.05,  # x
             0.93,  # y
             'ATLAS Open Data',  # text
             transform=main_axes.transAxes,  # coordinate system used is that of main_axes
             fontsize=13)
    
    # Add text 'for education' on plot
    plt.text(0.05,  # x
             0.88,  # y
             'for education',  # text
             transform=main_axes.transAxes,  # coordinate system used is that of main_axes
             style='italic',
             fontsize=8)
    
    # Add energy and luminosity
    lumi_used = str(lumi * fraction)  # Luminosity to write on the plot
    plt.text(0.05,  # x
             0.82,  # y
             '$\sqrt{s}$=13 TeV,$\int$L dt = ' + lumi_used + ' fb$^{-1}$',  # text
             transform=main_axes.transAxes)  # coordinate system used is that of main_axes
    
    # Add a label for the analysis carried out
    plt.text(0.05,  # x
             0.76,  # y
             r'$H \rightarrow ZZ^* \rightarrow 4\ell$',  # text
             transform=main_axes.transAxes)  # coordinate system used is that of main_axes
    
    # Draw the legend
    main_axes.legend(frameon=False)  # No box around the legend
    
    # Save plot
    output_dir = '/app/output'
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/mass_histogram.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    
    # Calculate signal significance
    signal_tot = signal_heights[0] + mc_x_tot
    bin_indices = [7, 8, 9]  # Bins around 125 GeV
    N_sig = signal_tot[bin_indices].sum()
    N_bg = mc_x_tot[bin_indices].sum()
    signal_significance = N_sig / np.sqrt(N_bg + 0.3 * N_bg**2)
    
    logging.info(f"\nResults:")
    logging.info(f"N_sig = {N_sig:.3f}")
    logging.info(f"N_bg = {N_bg:.3f}")
    logging.info(f"Signal significance = {signal_significance:.3f}")
    
    return {
        'N_sig': float(N_sig),
        'N_bg': float(N_bg),
        'significance': float(signal_significance),
        'plot_path': output_path
    }

def callback(ch, method, properties, body):
    """
    Callback function to process a visualization task from the queue.
    
    Args:
        ch: The RabbitMQ channel.
        method: The delivery method.
        properties: The message properties.
        body: The message body.
    """
    try:
        # Parse the task from the message body
        task = json.loads(body.decode())
        logging.info("Received visualization task")
        
        # Extract data
        plot_data = task['plot_data']
        bin_edges = np.array(task['bin_edges'])
        bin_centres = np.array(task['bin_centres'])
        lumi = task.get('lumi', 10)
        fraction = task.get('fraction', 1.0)
        
        # Create plot and calculate significance
        result = plot_mass_histogram(
            plot_data,
            bin_edges,
            bin_centres,
            step_size=5,
            lumi=lumi,
            fraction=fraction
        )
        
        logging.info(f"Visualization completed. Plot saved to {result['plot_path']}")
        logging.info(f"Signal significance: {result['significance']:.3f}")
        
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        logging.error(f"Error processing visualization task: {e}")
        # Acknowledge the message even on error to avoid reprocessing
        ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    """
    Main function to process visualization tasks from the queue.
    """
    # Connect to RabbitMQ
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    
    # Declare the visualization queue as durable
    channel.queue_declare(queue=VISUALIZATION_QUEUE, durable=True)
    
    # Set prefetch count to limit the number of unacknowledged messages
    channel.basic_qos(prefetch_count=1)
    
    # Set up the consumer with the callback function
    channel.basic_consume(queue=VISUALIZATION_QUEUE, on_message_callback=callback)
    
    logging.info("Visualization worker started. Waiting for tasks...")
    
    # Start consuming messages
    channel.start_consuming()

if __name__ == "__main__":
    main()