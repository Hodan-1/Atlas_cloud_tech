import pika
import json
import numpy as np
import matplotlib.pyplot as plt

class VisualizationMessenger:
    def __init__(self, rabbitmq_host='rabbitmq'):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
        self.channel = self.connection.channel()
        
        # Declare exchanges and queues
        self.channel.exchange_declare(exchange='visualization', exchange_type='topic')
        self.channel.queue_declare(queue='visualization_requests')
        self.channel.queue_declare(queue='visualization_responses')

    def send_plot_request(self, plot_data, bin_edges, bin_centres):
        """Send a plot generation request"""
        request = {
            'plot_data': plot_data,
            'bin_edges': bin_edges,
            'bin_centres': bin_centres
        }
        self.channel.basic_publish(
            exchange='visualization',
            routing_key='plot_request',
            body=json.dumps(request)
        )

    def listen_for_plot_requests(self):
        """Listen for plot generation requests"""
        def callback(ch, method, properties, body):
            request = json.loads(body)
            plot_data = request['plot_data']
            bin_edges = request['bin_edges']
            bin_centres = request['bin_centres']
            
            try:
                # Generate all plots
                plots = {
                    'data_only': plot_data_only(plot_data, bin_edges, bin_centres),
                    'data_with_background': plot_data_with_background(plot_data, bin_edges, bin_centres),
                    'full_mass_histogram': plot_full_mass_histogram(plot_data, bin_edges, bin_centres),
                    'lepton_pt_distributions': plot_lepton_pt_distributions(plot_data),
                    'final_analysis': plot_final_analysis(plot_data, bin_edges)
                }
                
                # Send response
                response = {
                    'status': 'success',
                    'plots': plots
                }
                self.channel.basic_publish(
                    exchange='visualization',
                    routing_key='plot_response',
                    body=json.dumps(response)
                )
            except Exception as e:
                # Handle error
                error_response = {
                    'status': 'error',
                    'error_message': str(e)
                }
                self.channel.basic_publish(
                    exchange='visualization',
                    routing_key='plot_response',
                    body=json.dumps(error_response)
                )

        self.channel.basic_consume(
            queue='visualization_requests', 
            on_message_callback=callback, 
            auto_ack=True
        )
        self.channel.start_consuming()