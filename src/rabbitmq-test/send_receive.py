import pika
import time
import multiprocessing
import os
from dotenv import load_dotenv

def consume(queue, priority):
    credentials = pika.PlainCredentials(os.environ["RABBITMQ_DEFAULT_USER"], os.environ["RABBITMQ_DEFAULT_PASS"])
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', credentials=credentials))
    channel = connection.channel()

    if priority:
        channel.queue_declare(queue=queue, durable=True, arguments={'x-max-priority': 5})
    else:
        channel.queue_declare(queue=queue, durable=True)

    callback = lambda ch, method, properties, body: print(f"Received: '{body}' in queue {queue} with routing key '{method.routing_key}'", flush=True)

    channel.basic_consume(queue=queue,
                          on_message_callback=callback,
                          auto_ack=True)

    print(f'Waiting for messages in queue: {queue}', flush=True)
    channel.start_consuming()

def publish(routing_key):
    credentials = pika.PlainCredentials(os.environ["RABBITMQ_DEFAULT_USER"], os.environ["RABBITMQ_DEFAULT_PASS"])
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', credentials=credentials))
    channel = connection.channel()

    message = f"Hello to {routing_key}!"
    channel.basic_publish(exchange='books-analyzer_exchange',
                          routing_key=routing_key,
                          body=message)
    print(f"Sent: '{message}' (msg) to '{routing_key}' (routing key).")

    connection.close()

def main():
    load_dotenv()

    # List of queues and corresponding routing keys
    queues = ['books-analyzer.data_processor.filter', 'books-analyzer.data_processor.counter', 'books-analyzer.data_processor.sentiment_analysis', 'books-analyzer.data_processor.joiner_books', 'books-analyzer.data_processor.joiner_reviews', 'books-analyzer.results']
    routing_keys = ['filter', 'counter', 'sentiment_analysis', 'joiner_books', 'joiner_reviews', 'results']

    for routing_key in routing_keys:
        publish(routing_key)
    
    for queue in queues:
        priority = not 'results' in queue
        multiprocessing.Process(target=consume, args=(queue, priority)).start()

if __name__ == "__main__":
    main()
