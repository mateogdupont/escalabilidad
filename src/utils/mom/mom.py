from typing import Any, Optional, Tuple
import pika
import os
from dotenv import load_dotenv
from utils.structs.data_chunk import DataChunk

class MOM:
    def __init__(self, consumer_queues: dict[str, bool]) -> None:
        load_dotenv()
        credentials = pika.PlainCredentials(os.environ["RABBITMQ_DEFAULT_USER"], os.environ["RABBITMQ_DEFAULT_PASS"])
        connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', credentials=credentials))
        self.channel = connection.channel()
        for queue, arguments in consumer_queues.items():
            arguments = {'x-max-priority': 5} if arguments else {}
            self.channel.queue_declare(queue=queue, durable=True, arguments=arguments)
        self.consumer_queues = consumer_queues.keys()
        self.exchange = os.environ["RABBITMQ_EXCHANGE"]
    
    def consume(self, queue: str) -> Optional[Tuple[DataChunk, int]]:
        if not queue in self.consumer_queues:
            raise ValueError(f"Queue '{queue}' not found in consumer_queues.")
        method, _, body = self.channel.basic_get(queue=queue, auto_ack=False)
        if not method:
            return None
        data_chunk = DataChunk.from_json(body)
        return data_chunk, method.delivery_tag
    
    def ack(self, delivery_tag: int) -> None:
        self.channel.basic_ack(delivery_tag=delivery_tag)
    
    def publish(self, data_chunk: DataChunk, key: str) -> None:
        self.channel.basic_publish(exchange=self.exchange,
                                routing_key=key,
                                body=data_chunk.to_json())