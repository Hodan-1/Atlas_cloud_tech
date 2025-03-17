import pika
import os

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue='tasks')

# List of data files to process
data_files = ["data_A.4lep.root", "data_B.4lep.root"]

# Send tasks to the queue
for file in data_files:
    task = {'file_path': file}
    channel.basic_publish(exchange='', routing_key='tasks', body=str(task))
    print(f"Sent task: {task}")

connection.close()
