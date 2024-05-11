from typing import Any, Optional, Tuple
import pika
import os
from dotenv import load_dotenv
from utils.structs.data_chunk import DataChunk
import logging as logger
from time import sleep

MAX_TRIES = 5

class MOM:
    def __init__(self, consumer_queues: 'dict[str, bool]') -> None:
        load_dotenv()
        if not self._connect():
            return
        self.channel.basic_qos(prefetch_count=1)
        for queue, arguments in consumer_queues.items():
            arguments = {'x-max-priority': 5} if arguments else {}
            self.channel.queue_declare(queue=queue, durable=True, arguments=arguments)
        self.consumer_queues = consumer_queues.keys()
        self.exchange = os.environ["RABBITMQ_EXCHANGE"]
        self.consumer_queues = consumer_queues
    
    def _connect(self) -> bool:
        last_exception = None
        for _ in range(MAX_TRIES):
            try:
                credentials = pika.PlainCredentials(os.environ["RABBITMQ_DEFAULT_USER"], os.environ["RABBITMQ_DEFAULT_PASS"])
                self.connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', credentials=credentials, heartbeat=60*60))
                self.channel = self.connection.channel()
                return True
            except Exception as e:
                last_exception = e
                sleep(0.1) # wait a little before trying again, perhaps the service is not up yet
        logger.error(f"Cannot connect to RabbitMQ, the last exception was: {last_exception}")
        return False
    
    def _re_connect(self):
        logger.warning(f"Reconnecting to RabbitMQ")
        self.close()
        self._connect()
        self.channel.basic_qos(prefetch_count=1)
        for queue, arguments in self.consumer_queues.items():
            arguments = {'x-max-priority': 5} if arguments else {}
            self.channel.queue_declare(queue=queue, durable=True, arguments=arguments)
    
    def _execute(self, method: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return method(*args, **kwargs)
        except Exception as e:
            self._re_connect()
            return method(*args, **kwargs)
        
    def consume(self, queue: str) -> Optional[Tuple[DataChunk, int]]:
        if not queue in self.consumer_queues:
            raise ValueError(f"Queue '{queue}' not found in consumer_queues.")
        return self._execute(self._consume, queue)
    
    def _consume(self, queue: str) -> Optional[Tuple[DataChunk, int]]:
        method, _, body = self.channel.basic_get(queue=queue, auto_ack=False)
        if not method:
            return None
        data_chunk = DataChunk.from_json(body)
        return data_chunk, method.delivery_tag
    
    def ack(self, delivery_tag: int) -> None:
        self._execute(self._ack, delivery_tag)
    
    def _ack(self, delivery_tag: int) -> None:
        self.channel.basic_ack(delivery_tag=delivery_tag)
    
    def nack(self, delivery_tag: int) -> None:
        self._execute(self._nack, delivery_tag)

    def _nack(self, delivery_tag: int) -> None:
        self.channel.basic_nack(delivery_tag=delivery_tag)
    
    def publish(self, data_chunk: DataChunk, key: str) -> None:
        if data_chunk is None or data_chunk.get_fragments() is None or len(data_chunk.get_fragments()) == 0 or data_chunk.to_json() is None:
            logger.error(f"DataChunk is None")
        self._execute(self._publish, data_chunk, key)
            
    def _publish(self, data_chunk: DataChunk, key: str) -> None:
        self.channel.basic_publish(exchange=self.exchange,
                                    routing_key=key,
                                    body=data_chunk.to_json())

    def close(self):
        try:
            self.channel.close()
            self.connection.close()
        except Exception as e:
            pass
            