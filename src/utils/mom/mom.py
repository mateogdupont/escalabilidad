from typing import Any, Optional, Tuple
import pika
import os
from dotenv import load_dotenv
from utils.structs.data_fragment import DataFragment
import logging as logger

class MOMNotAvailableError(Exception):
    """Raised when the Message Oriented Middleware (MOM) is not available."""
    pass

class MOM:
    def __init__(self, consumer_queues: 'dict[str, bool]') -> None:
        load_dotenv()
        if not self._connect(0):
            raise MOMNotAvailableError("The connection to the Message Oriented Middleware (MOM) is not available.")
        self.channel.basic_qos(prefetch_count=1)
        for queue, arguments in consumer_queues.items():
            arguments = {'x-max-priority': 5} if arguments else {}
            self.channel.queue_declare(queue=queue, durable=True, arguments=arguments)
        self.consumer_queues = consumer_queues.keys()
        self.exchange = os.environ["RABBITMQ_EXCHANGE"]

    def _connect(self, retry: int) -> bool:
        try:
            credentials = pika.PlainCredentials(os.environ["RABBITMQ_DEFAULT_USER"], os.environ["RABBITMQ_DEFAULT_PASS"])
            connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', credentials=credentials))
            self.channel = connection.channel()
            return True
        except Exception as e:
            if retry == os.environ["MAX_RETRIES"]:
                logger.error(f"Max retries limit reached, ending the connection with rabbitmq. Last error was '{e}'")
                return False
            return self._connect(retry + 1)
    
    def consume(self, queue: str) -> Optional[Tuple[DataFragment, int]]:
        if not queue in self.consumer_queues:
            raise ValueError(f"Queue '{queue}' not found in consumer_queues.")
        method, _, body = self.channel.basic_get(queue=queue, auto_ack=False)
        if not method:
            return None
        data_fragment = DataFragment.from_json(body)
        return data_fragment, method.delivery_tag
    
    def ack(self, delivery_tag: int) -> None:
        self.channel.basic_ack(delivery_tag=delivery_tag)
    
    def publish(self, data_fragments: 'dict[DataFragment, str]') -> None:
        for data_fragment, routing_key in data_fragments.items():
            self.channel.basic_publish(exchange=self.exchange,
                                    routing_key=routing_key,
                                    body=data_fragment.to_json())
        
    def __del__(self):
        self.channel.close()
        self.connection.close()
