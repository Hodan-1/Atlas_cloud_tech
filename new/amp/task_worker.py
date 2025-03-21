import os
import pika
import json
from data_load import load_file
from data_processor import process_data
from analysis import prepare_plot_data
from visualisation import setup_histogram_bins, plot_data_only, plot_data_with_background, plot_full_mass_histogram, plot_lepton_pt_distributions, plot_final_analysis

RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
TASK_QUEUE = os.getenv('TASK_QUEUE', 'task_queue')

def callback(ch, method, properties, body):
    """Callback function to process tasks from RabbitMQ."""
    task_data = json.loads(body)
    sample_type = task_data['sample_type']
    sample_name = task_data['sample_name']
    
    print(f" [x] Received {task_data}")
    tree = load_file(sample_type, sample_name)
    is_mc = sample_type != 'data'
    processed_data = process_data(tree, sample_name, is_mc)

    if processed_data is not None:
        print(f"Processed {sample_name}: {len(processed_data)} entries.")
        all_data = {'data': processed_data}
        
        bin_edges, bin_centres = setup_histogram_bins()
        plot_data_only(all_data, bin_edges, bin_centres)
        plot_data_with_background(all_data, bin_edges, bin_centres)
        plot_full_mass_histogram(all_data, bin_edges, bin_centres)
        plot_lepton_pt_distributions(all_data)
        plot_final_analysis(all_data, bin_edges)

def consume_tasks():
    """Consume tasks from RabbitMQ."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=TASK_QUEUE)

    channel.basic_consume(queue=TASK_QUEUE, on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == "__main__":
    consume_tasks()