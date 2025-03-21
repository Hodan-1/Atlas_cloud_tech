# workers/analyzer/analyzer.py
import json
import time
import numpy as np
import awkward as ak
import pika
from common import connect_to_rabbitmq, serialize_data, deserialize_data
from common import ANALYSIS_QUEUE, RESULT_QUEUE
from constants import MeV, GeV

def calculate_histogram_data(data, bin_edges):
    """Calculate histogram data and errors"""
    data_x, _ = np.histogram(ak.to_numpy(data['mass']), bins=bin_edges)
    data_x_errors = np.sqrt(data_x)
    return data_x, data_x_errors

def callback(ch, method, properties, body):
    """Process an analysis task from the queue"""
    try:
        # Parse task
        task = json.loads(body.decode())
        sample_type = task['sample_type']
        sample_name = task['sample_name']
        task_id = task['task_id']
        data = deserialize_data(task['data'])
        lumi = task.get('lumi', 10)
        fraction = task.get('fraction', 1.0)
        
        print(f"Analyzing data for {sample_name}")
        start_time = time.time()
        
        # Create a result task
        result_task = {
            'task_id': task_id,
            'sample_type': sample_type,
            'sample_name': sample_name,
            'data': serialize_data(data),
            'lumi': lumi,
            'fraction': fraction
        }
        
        # Send to result queue
        channel = ch.connection.channel()
        channel.queue_declare(queue=RESULT_QUEUE, durable=True)
        channel.basic_publish(
            exchange='',
            routing_key=RESULT_QUEUE,
            body=json.dumps(result_task, default=str),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                content_type='application/json'
            )
        )
        
        elapsed = time.time() - start_time
        print(f"Analyzed {sample_name} in {round(elapsed, 1)}s")
        
        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    except Exception as e:
        print(f"Error analyzing data for {task['sample_name']}: {e}")
        # Acknowledge the message even on error to avoid reprocessing
        ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    """Main function to process analysis tasks from the queue"""
    # Connect to RabbitMQ
    connection = connect_to_rabbitmq()
    channel = connection.channel()
    
    # Declare queue
    channel.queue_declare(queue=ANALYSIS_QUEUE, durable=True)
    
    # Set prefetch count
    channel.basic_qos(prefetch_count=1)
    
    # Set up consumer
    channel.basic_consume(queue=ANALYSIS_QUEUE, on_message_callback=callback)
    
    print("Analyzer worker started. Waiting for tasks...")
    
    # Start consuming
    channel.start_consuming()

if __name__ == "__main__":
    main()
