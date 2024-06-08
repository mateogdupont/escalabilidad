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
        self._connect()
        self.channel.basic_qos(prefetch_count=1)
        for queue, arguments in consumer_queues.items():
            arguments = {'x-max-priority': 5} if arguments else {}
            self.channel.queue_declare(queue=queue, durable=True, arguments=arguments)
        self.consumer_queues = consumer_queues.keys()
        self.exchange = os.environ["RABBITMQ_EXCHANGE"]
        self.consumer_queues = consumer_queues
    
    def _connect(self) -> None:
        last_exception = None
        for _ in range(MAX_TRIES):
            try:
                credentials = pika.PlainCredentials(os.environ["RABBITMQ_DEFAULT_USER"], os.environ["RABBITMQ_DEFAULT_PASS"])
                self.connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', credentials=credentials, heartbeat=60*60))
                self.channel = self.connection.channel()
                return
            except Exception as e:
                last_exception = e
                sleep(0.1) # wait a little before trying again, perhaps the service is not up yet
        logger.error(f"Cannot connect to RabbitMQ, the last exception was: {last_exception}")
        raise last_exception
    
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
        data_chunk = DataChunk.from_str(body)
        return data_chunk, method.delivery_tag
    
    def ack(self, delivery_tag: int) -> None:
        self._execute(self._ack, delivery_tag)
    
    def _ack(self, delivery_tag: int) -> None:
        self.channel.basic_ack(delivery_tag=delivery_tag)
    
    def nack(self, delivery_tag: int) -> None:
        self._execute(self._nack, delivery_tag)

    def _nack(self, delivery_tag: int) -> None:
        self.channel.basic_nack(delivery_tag=delivery_tag)
    
    def publish(self, data_chunk: DataChunk, key: str, priority: int = 1) -> None:
        if data_chunk is None or data_chunk.get_fragments() is None or len(data_chunk.get_fragments()) == 0 or data_chunk.to_str() is None:
            logger.error(f"DataChunk is None")
        self._execute(self._publish, data_chunk, key, priority)
            
    def _publish(self, data_chunk: DataChunk, key: str, priority: int = 1) -> None:
        self.channel.basic_publish(exchange=self.exchange,
                                    routing_key=key,
                                    body=data_chunk.to_str(),
                                    properties=pika.BasicProperties(
                                        delivery_mode=2,
                                        priority=priority
                                    ))
    
    def publish_log(self, queue_name: str, message: str, priority: int = 1) -> None:
        self._execute(self._publish_log, queue_name, message, priority)

    def _publish_log(self, queue_name: str, message: str, priority: int = 1) -> None:
        self.channel.basic_publish(exchange="",
                                      routing_key=queue_name,
                                      body=message,
                                        properties=pika.BasicProperties(
                                            delivery_mode=2,
                                            priority=priority
                                        ))
    
    def publish_log_global(self, key: str, message: str, priority: int = 1) -> None:
        self._execute(self._publish_log_global, key, message, priority)

    def _publish_log_global(self, key: str, message: str, priority: int = 1) -> None:
        self.channel.basic_publish(exchange=self.exchange,
                                      routing_key=key,
                                      body=message,
                                        properties=pika.BasicProperties(
                                            delivery_mode=2,
                                            priority=priority
                                        ))

    def close(self):
        try:
            self.channel.close()
            self.connection.close()
        except Exception as e:
            pass
            