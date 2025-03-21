import pika
import json
import time
from data_loader import samples

RABBITMQ_HOST = 'rabbitmq'
TASK_QUEUE = 'task_queue'

def publish_task(sample_type, sample_name):
    """Publish a task to RabbitMQ."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=TASK_QUEUE)

    task_data = {'sample_type': sample_type, 'sample_name': sample_name}
    
    channel.basic_publish(exchange='', routing_key=TASK_QUEUE, body=json.dumps(task_data))
    print(f" [x] Sent {task_data}")
    connection.close()

def main(samples):
    """Main function to publish all tasks."""
    for sample_type in samples:
        for sample_name in samples[sample_type]['list']:
            publish_task(sample_type, sample_name)
            time.sleep(1)  # Stagger task submissions

if __name__ == "__main__":
    main(samples)