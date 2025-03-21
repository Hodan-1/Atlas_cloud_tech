import pika
import json
import numpy as np
import uproot
import awkward as ak

class DataLoaderMessenger:
    def __init__(self, rabbitmq_host='rabbitmq'):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
        self.channel = self.connection.channel()
        
        # Declare exchanges and queues
        self.channel.exchange_declare(exchange='data_loader', exchange_type='topic')
        self.channel.queue_declare(queue='data_loader_requests')
        self.channel.queue_declare(queue='data_loader_responses')

    def send_file_load_request(self, sample_type, sample_name):
        """Send a request to load a specific file"""
        request = {
            'sample_type': sample_type,
            'sample_name': sample_name
        }
        self.channel.basic_publish(
            exchange='data_loader',
            routing_key='load_request',
            body=json.dumps(request)
        )

    def listen_for_load_requests(self):
        """Listen for file load requests"""
        def callback(ch, method, properties, body):
            request = json.loads(body)
            sample_type = request['sample_type']
            sample_name = request['sample_name']
            
            # Actual file loading logic
            try:
                if sample_type == 'data':
                    prefix = "Data/"
                    file_path = path + prefix + sample_name + ".4lep.root"
                else:
                    prefix = "MC/mc_" + str(infofile.infos[sample_name]["DSID"]) + "."
                    file_path = path + prefix + sample_name + ".4lep.root"
                
                tree = uproot.open(file_path + ":mini")
                
                # Send response
                response = {
                    'sample_type': sample_type,
                    'sample_name': sample_name,
                    'status': 'success'
                }
                self.channel.basic_publish(
                    exchange='data_loader',
                    routing_key='load_response',
                    body=json.dumps(response)
                )
            except Exception as e:
                # Handle error
                error_response = {
                    'sample_type': sample_type,
                    'sample_name': sample_name,
                    'status': 'error',
                    'error_message': str(e)
                }
                self.channel.basic_publish(
                    exchange='data_loader',
                    routing_key='load_response',
                    body=json.dumps(error_response)
                )

        self.channel.basic_consume(
            queue='data_loader_requests', 
            on_message_callback=callback, 
            auto_ack=True
        )
        self.channel.start_consuming()