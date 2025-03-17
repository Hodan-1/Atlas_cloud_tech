import pika
import uproot
import numpy as np

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue='tasks')
channel.queue_declare(queue='results')

def process_task(task):
    # Example: Process a subset of data
    file_path = task['file_path']
    tree = uproot.open(file_path + ":mini")
    data = tree.arrays()
    # Perform analysis (e.g., calculate invariant mass)
    result = np.mean(data['lep_pt'])  # Example calculation
    return result

def callback(ch, method, properties, body):
    task = eval(body.decode())
    result = process_task(task)
    # Send result back to results queue
    channel.basic_publish(exchange='', routing_key='results', body=str(result))
    print(f"Processed task: {task}")

channel.basic_consume(queue='tasks', on_message_callback=callback, auto_ack=True)
print('Worker waiting for tasks...')
channel.start_consuming()
