import os
import pika
import json
from data_loader import DataLoaderMessenger
from data_processor import DataProcessorMessenger
from analysis import AnalysisMessenger
from visualization import VisualizationMessenger
from constants import SAMPLES, MeV, GeV

class MainOrchestrator:
    def __init__(self, rabbitmq_host='rabbitmq'):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(rabbitmq_host))
        self.channel = self.connection.channel()
        
        # Declare main orchestration exchange and queue
        self.channel.exchange_declare(exchange='main_orchestrator', exchange_type='topic')
        self.channel.queue_declare(queue='orchestrator_requests')

        # Initialize messengers for each component
        self.data_loader_messenger = DataLoaderMessenger(rabbitmq_host)
        self.data_processor_messenger = DataProcessorMessenger(rabbitmq_host)
        self.analysis_messenger = AnalysisMessenger(rabbitmq_host)
        self.visualization_messenger = VisualizationMessenger(rabbitmq_host)

    def start_processing_pipeline(self, samples):
        """Initiate the entire processing pipeline via messaging"""
        for sample_type, sample_info in samples.items():
            for sample_name in sample_info['list']:
                # Send load request
                self.data_loader_messenger.send_file_load_request(sample_type, sample_name)

    def listen_for_orchestration_requests(self):
        """Listen for high-level orchestration requests"""
        def callback(ch, method, properties, body):
            request = json.loads(body)
            # Handle different types of orchestration requests
            if request['type'] == 'start_pipeline':
                self.start_processing_pipeline(request['samples'])

        self.channel.basic_consume(
            queue='orchestrator_requests', 
            on_message_callback=callback, 
            auto_ack=True
        )
        self.channel.start_consuming()

def main():
    orchestrator = MainOrchestrator()
    
    # Determine role based on environment
    role = os.environ.get('ROLE', 'worker')
    
    if role == 'publisher':
        # Publish initial tasks
        orchestrator.start_processing_pipeline(samples)
    else:
        # Start listening for orchestration requests
        orchestrator.listen_for_orchestration_requests()

if __name__ == "__main__":
    main()