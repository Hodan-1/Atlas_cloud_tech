import os
import json
import time
import uuid
import pika
import numpy as np
import logging
from common import connect_to_rabbitmq, serialize_data, deserialize_data
from common import SAMPLE_QUEUE, RESULT_QUEUE, PLOT_QUEUE, PROCESS_QUEUE, ANALYSIS_QUEUE
from constants import SAMPLES

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_histogram_bins(xmin=80*0.001, xmax=250*0.001, step_size=5*0.001):
    """Set up histogram bins"""
    bin_edges = np.arange(start=xmin, stop=xmax+step_size, step=step_size)
    bin_centres = np.arange(start=xmin+step_size/2, stop=xmax+step_size/2, step=step_size)
    return bin_edges, bin_centres

def setup_queues(channel):
    """Set up all required queues"""
    channel.queue_declare(queue=SAMPLE_QUEUE, durable=True)
    channel.queue_declare(queue=PROCESS_QUEUE, durable=True)
    channel.queue_declare(queue=ANALYSIS_QUEUE, durable=True)
    channel.queue_declare(queue=RESULT_QUEUE, durable=True)
    channel.queue_declare(queue=PLOT_QUEUE, durable=True)
    logger.info("Queues declared successfully")

def submit_sample_tasks(channel, lumi=10, fraction=1.0):
    """Submit tasks to process all samples"""
    task_id = str(uuid.uuid4())
    logger.info(f"Submitting tasks with ID: {task_id}")
    
    for sample_type, sample_info in SAMPLES.items():
        for sample_name in sample_info['list']:
            task = {
                'task_id': task_id,
                'sample_type': sample_type,
                'sample_name': sample_name,
                'lumi': lumi,
                'fraction': fraction
            }
            
            channel.basic_publish(
                exchange='',
                routing_key=SAMPLE_QUEUE,
                body=json.dumps(task),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                    content_type='application/json'
                )
            )
            logger.info(f"Submitted task for {sample_type} - {sample_name}")
    
    return task_id

def collect_results(connection, task_id, timeout=3600):
    """Collect results for a specific task ID"""
    channel = connection.channel()
    channel.queue_declare(queue=RESULT_QUEUE, durable=True)
    
    results = {}
    expected_samples = sum(len(sample_info['list']) for sample_info in SAMPLES.values())
    received_samples = 0
    
    start_time = time.time()
    
    def callback(ch, method, properties, body):
        nonlocal received_samples
        
        task = json.loads(body.decode())
        if task['task_id'] == task_id:
            sample_type = task['sample_type']
            sample_name = task['sample_name']
            data = deserialize_data(task['data'])
            
            logger.info(f"Received result for {sample_type} - {sample_name}")
            
            if sample_type not in results:
                results[sample_type] = data
            else:
                import awkward as ak
                results[sample_type] = ak.concatenate([results[sample_type], data])
            
            received_samples += 1
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            if received_samples >= expected_samples:
                ch.stop_consuming()
        else:
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    channel.basic_consume(queue=RESULT_QUEUE, on_message_callback=callback)
    
    while received_samples < expected_samples:
        if time.time() - start_time > timeout:
            logger.warning(f"Timeout waiting for results. Received {received_samples}/{expected_samples}")
            break
        
        connection.process_data_events(time_limit=1)
        time.sleep(0.1)
    
    return results

def submit_plot_task(channel, results, lumi=10, fraction=1.0):
    """Submit a task to create plots"""
    bin_edges, bin_centres = setup_histogram_bins()
    
    task = {
        'results': serialize_data(results),
        'bin_edges': bin_edges.tolist(),
        'bin_centres': bin_centres.tolist(),
        'lumi': lumi,
        'fraction': fraction
    }
    
    channel.basic_publish(
        exchange='',
        routing_key=PLOT_QUEUE,
        body=json.dumps(task, default=str),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
            content_type='application/json'
        )
    )
    logger.info("Submitted plot task")

def main():
    """Main coordinator function"""
    try:
        connection = connect_to_rabbitmq()
        channel = connection.channel()
        
        setup_queues(channel)
        
        lumi = 10
        fraction = 1.0
        task_id = submit_sample_tasks(channel, lumi, fraction)
        
        logger.info("Waiting for results...")
        results = collect_results(connection, task_id)
        
        if results:
            logger.info("All results collected. Submitting plot task...")
            submit_plot_task(channel, results, lumi, fraction)
        else:
            logger.warning("No results collected. Exiting.")
        
        connection.close()
    except Exception as e:
        logger.error(f"Error in coordinator: {e}")
        raise

if __name__ == "__main__":
    main()