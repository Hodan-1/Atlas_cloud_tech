import pika
import matplotlib.pyplot as plt

# Connect to RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
channel = connection.channel()
channel.queue_declare(queue='results')

results = []

def callback(ch, method, properties, body):
    result = float(body.decode())
    results.append(result)
    print(f"Received result: {result}")
    if len(results) == 2:  # Example: Stop after 2 results
        # Generate final plot
        plt.hist(results, bins=10)
        plt.savefig('final_plot.png')
        print("Final plot saved as final_plot.png")
        connection.close()

channel.basic_consume(queue='results', on_message_callback=callback, auto_ack=True)
print('Aggregator waiting for results...')
channel.start_consuming()
