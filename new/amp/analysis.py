import pika
import json
import numpy as np
import awkward as ak
from data_processing_utils import prepare_plot_data, calculate_signal_significance

class AnalysisMessenger:
    def __init__(self, rabbitmq_host='rabbitmq'):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
        self.channel = self.connection.channel()
        
        # Declare exchanges and queues
        self.channel.exchange_declare(exchange='analysis', exchange_type='topic')
        self.channel.queue_declare(queue='analysis_requests')
        self.channel.queue_declare(queue='analysis_responses')

    def send_analysis_request(self, all_data, samples, bin_edges):
        """Send an analysis request"""
        request = {
            'all_data': all_data,
            'samples': samples,
            'bin_edges': bin_edges
        }
        self.channel.basic_publish(
            exchange='analysis',
            routing_key='analysis_request',
            body=json.dumps(request)
        )

    def listen_for_analysis_requests(self):
        """Listen for analysis requests"""
        def callback(ch, method, properties, body):
            request = json.loads(body)
            all_data = request['all_data']
            samples = request['samples']
            bin_edges = request['bin_edges']
            
            try:
                # Actual analysis logic
                plot_data = prepare_plot_data(all_data, samples, bin_edges)
                signal_significance = calculate_signal_significance(
                    plot_data['signal_x'], 
                    plot_data['mc_x'], 
                    [7, 8, 9]  # Example bin indices
                )
                
                # Send response
                response = {
                    'status': 'success',
                    'plot_data': plot_data,
                    'signal_significance': signal_significance
                }
                self.channel.basic_publish(
                    exchange='analysis',
                    routing_key='analysis_response',
                    body=json.dumps(response)
                )
            except Exception as e:
                # Handle error
                error_response = {
                    'status': 'error',
                    'error_message': str(e)
                }
                self.channel.basic_publish(
                    exchange='analysis',
                    routing_key='analysis_response',
                    body=json.dumps(error_response)
                )

        self.channel.basic_consume(
            queue='analysis_requests', 
            on_message_callback=callback, 
            auto_ack=True
        )
        self.channel.start_consuming()