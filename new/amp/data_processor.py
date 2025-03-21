import pika
import json
import numpy as np
import awkward as ak
import vector
from data_processing_utils import process_data

class DataProcessorMessenger:
    def __init__(self, rabbitmq_host='rabbitmq'):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
        self.channel = self.connection.channel()
        
        # Declare exchanges and queues
        self.channel.exchange_declare(exchange='data_processor', exchange_type='topic')
        self.channel.queue_declare(queue='data_processor_requests')
        self.channel.queue_declare(queue='data_processor_responses')

    def send_process_request(self, tree, sample_name, is_mc=False):
        """Send a processing request"""
        request = {
            'sample_name': sample_name,
            'is_mc': is_mc
        }
        self.channel.basic_publish(
            exchange='data_processor',
            routing_key='process_request',
            body=json.dumps(request)
        )

    def listen_for_process_requests(self):
        """Listen for processing requests"""
        def callback(ch, method, properties, body):
            request = json.loads(body)
            sample_name = request['sample_name']
            is_mc = request['is_mc']
            
            try:
                # Actual processing logic
                processed_data = process_data(tree, sample_name, is_mc)
                
                # Send response
                response = {
                    'sample_name': sample_name,
                    'status': 'success',
                    'processed_data': processed_data.tolist() if processed_data is not None else None
                }
                self.channel.basic_publish(
                    exchange='data_processor',
                    routing_key='process_response',
                    body=json.dumps(response)
                )
            except Exception as e:
                # Handle error
                error_response = {
                    'sample_name': sample_name,
                    'status': 'error',
                    'error_message': str(e)
                }
                self.channel.basic_publish(
                    exchange='data_processor',
                    routing_key='process_response',
                    body=json.dumps(error_response)
                )

        self.channel.basic_consume(
            queue='data_processor_requests', 
            on_message_callback=callback, 
            auto_ack=True
        )
        self.channel.start_consuming()