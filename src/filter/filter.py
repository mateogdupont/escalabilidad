import signal
import os
import time
import socket
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.structs.data_chunk import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step
from multiprocessing import Process, Event
from dotenv import load_dotenv # type: ignore
import logging as logger
import sys
from log_manager.log_writer import *
from log_manager.log_recoverer import *

load_dotenv()
CATEGORY_FILTER = "CATEGORY"
YEAR_FILTER = "YEAR"
TITLE_FILTER = "TITLE"
DISTINCT_FILTER = "COUNT_DISTINCT"
SENTIMENT_FILTER = "SENTIMENT"
NODE_TYPE=os.environ["NODE_TYPE"]
HARTBEAT_INTERVAL=int(os.environ["HARTBEAT_INTERVAL"])
MAX_AMOUNT_OF_FRAGMENTS = 800
TIMEOUT = 50

class Filter:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        log_recoverer = LogRecoverer(os.environ["LOG_PATH"])
        log_recoverer.recover_data()
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.work_queue = consumer_queues[0]
        self.info_queue = os.environ["INFO_QUEUE"]
        self.info_key = os.environ["INFO_KEY"]
        self.nodes = int(os.environ["FILTER_NODES"])
        consumer_queues.append(self.info_queue)
        self.mom = MOM(consumer_queues)
        self.results = log_recoverer.get_results()
        self.received_ids = log_recoverer.get_received_ids()
        self.ignore_ids = log_recoverer.get_ignore_ids()
        self.event = None
        self.medic_addres = (os.environ["MEDIC_IP"], int(os.environ["MEDIC_PORT"]))
        self.id= os.environ["ID"]
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)
        self.exit = False
        self.log_writer = LogWriter(os.environ["LOG_PATH"])

    def clean_data_client(self, client_id):
        if client_id in self.received_ids.keys():
            self.received_ids.pop(client_id)
        for node, batch in self.results.items():
            batch = ([fragment for fragment in batch[0] if fragment.get_client_id() != client_id], batch[1])
            self.results[node] = batch
        self.ignore_ids.add(client_id)
        self.log_writer.log_ignore(client_id)
    
    def sigterm_handler(self, signal,frame):
        self.exit = True
        if self.mom:
            self.mom.close()
        if self.log_writer:
            self.log_writer.close()
        if self.event:
            self.event.set()

    def save_id(self, data_fragment: DataFragment) -> bool:
        client_id = data_fragment.get_client_id()
        if client_id in self.ignore_ids:
            return False
        query_id = data_fragment.get_query_id()
        id = data_fragment.get_id()
        self.received_ids[client_id] = self.received_ids.get(client_id, {})
        self.received_ids[client_id][query_id] = self.received_ids[client_id].get(query_id, set())
        if id in self.received_ids[client_id][query_id]:
            return False
        self.received_ids[client_id][query_id].add(id)
        return True

    def filter_data_fragment(self, data_fragment: DataFragment, event) -> bool:
        query_info = data_fragment.get_query_info()
        filter_on, word, min_value, max_value = query_info.get_filter_params()
        book = data_fragment.get_book()

        if (filter_on == CATEGORY_FILTER) and (book is not None):
            return word.lower() in [c.lower() for c in book.get_categories()]
        elif (filter_on == YEAR_FILTER) and (book is not None):
            book_year = book.get_published_year()
            return min_value <= book_year <= max_value
        elif (filter_on == TITLE_FILTER) and (book is not None):
            return word.lower() in book.get_title().lower()
        elif filter_on == DISTINCT_FILTER and (query_info.get_n_distinct() is not None):
            return query_info.get_n_distinct() >= min_value
        elif filter_on == SENTIMENT_FILTER and (query_info.get_sentiment() is not None):
            return query_info.get_sentiment() >= min_value
        
        return False

    def add_and_try_to_send_chunk(self, fragment: DataFragment, node: str,event):
        if event.is_set():
            return
        if not node in self.results.keys():
            self.results[node] = ([], time.time())
        self.results[node][0].append(fragment)
        self.results[node] = (self.results[node][0], time.time())
        if len(self.results[node][0]) == MAX_AMOUNT_OF_FRAGMENTS or fragment.is_last():
            data_chunk = DataChunk(self.results[node][0])
            try:
                self.mom.publish(data_chunk, node)
                self.log_writer.log_result_sent(node)
            except Exception as e:
                logger.error(f"Error al enviar a {node}: {e}")
                logger.error(f"Data: {data_chunk.to_bytes()}")
            self.results[node] = ([], time.time())
        
    def filter_data_chunk(self,chunk: DataChunk, event):
        for fragment in chunk.get_fragments():
            if event.is_set():
                return
            if not self.save_id(fragment):
                continue
            if (not fragment.is_last()):
                if self.filter_data_fragment(fragment,event):
                    next_steps = update_data_fragment_step(fragment)
                    if len(next_steps.items()) == 0:
                        logger.info(f"Fragmento {fragment} no tiene siguiente paso")
                    list_next_steps = [(fragment, key) for fragment, key in next_steps.items()]
                    self.log_writer.log_result(list_next_steps, time=time.time())
                    for data, key in next_steps.items():
                        self.add_and_try_to_send_chunk(data, key, event)
                else:
                    self.log_writer.log_received_id(fragment)
            if fragment.is_last():
                self.sync_last(fragment)
                self.log_writer.log_query_ended(fragment)
                next_steps = update_data_fragment_step(fragment)
                for data, key in next_steps.items():
                    self.add_and_try_to_send_chunk(data, key, event)
    
    def get_sync_id(self, data_fragment: DataFragment) -> int:
        client_id = data_fragment.get_client_id()
        query_id = data_fragment.get_query_id()
        all_id = client_id + query_id
        encoded_id = all_id.encode()
        return int.from_bytes(encoded_id, byteorder='big')
    
    def sync_last(self, last_data_fragment: DataFragment) -> None:
        logger.info("I have the last, before send it I will sync")
        sync_fragment = last_data_fragment.clone()
        sync_fragment.set_sync(True, False)
        self.mom.publish(sync_fragment, self.info_key)
        logger.info(f"Sync sent (sync_id = {self.get_sync_id(sync_fragment)})")
        client_id = last_data_fragment.get_client_id()
        query_id = last_data_fragment.get_query_id()
        nodes_left = self.nodes
        while nodes_left > 0:
            msg = self.mom.consume(self.info_queue)
            if not msg:
                time.sleep(0.5)
                continue
            datafragment, tag = msg
            if datafragment.get_query_info().is_clean_flag():
                self.mom.nack(tag)
                logger.info("Received a clean flag, postponing (nack sent)")
                time.sleep(0.5)
                continue
            start_sync, end_sync = datafragment.get_sync()
            if end_sync and datafragment.get_client_id() == client_id and datafragment.get_query_id() == query_id:
                nodes_left -= 1
                logger.info(f"Sync response received, {nodes_left} nodes left (sync_id = {self.get_sync_id(sync_fragment)})")
            elif start_sync:
                logger.info(f"Received a sync request, sending data fragments (sync_id = {self.get_sync_id(sync_fragment)})")
                keys = update_data_fragment_step(datafragment.clone()).keys()
                for key in keys:
                    self.force_send(key)
                datafragment.set_sync(False, True)
                self.mom.publish(datafragment, self.info_key)
                logger.info("Data fragments sent, sync response sent")
            self.mom.ack(tag)
        logger.info("All nodes synced, ready to send last fragment")

    def force_send(self, node: str) -> None:
        if not node in self.results.keys():
            return
        if len(self.results[node][0]) > 0:
            data_chunk = DataChunk(self.results[node][0])
            try:
                self.mom.publish(data_chunk, node)
                self.log_writer.log_result_sent(node)
            except Exception as e:
                logger.error(f"Error al enviar a {node}: {e}")
                logger.error(f"Data: {data_chunk.to_bytes()}")
            self.results[node] = ([], time.time())

    def send_with_timeout(self,event):
        for key, (data, last_sent) in self.results.items():
            if event.is_set():
                return
            if (len(data) > 0) and (time.time() - last_sent > TIMEOUT):
                chunk = DataChunk(data)
                try:
                    self.mom.publish(chunk, key)
                    self.log_writer.log_result_sent(key)
                except Exception as e:
                    logger.error(f"Error al enviar a {key}: {e}")
                    logger.error(f"Data: {chunk.to_bytes()}")
                self.results[key] = ([], time.time())

    def callback(self, ch, method, properties, body,event):
        self.inspect_info_queue(event)
        data_chunk = DataChunk.from_bytes(body)
        self.filter_data_chunk(data_chunk,event)
        self.mom.ack(delivery_tag=method.delivery_tag)
        self.send_with_timeout(event)

    def inspect_info_queue(self, event) -> None:
        while not event.is_set():
            msg = self.mom.consume(self.info_queue)
            if not msg:
                return
            datafragment, tag = msg
            start_sync, end_sync = datafragment.get_sync()
            if datafragment.get_query_info().is_clean_flag():
                client_id = datafragment.get_client_id()
                logger.info(f"Received a clean flag for client {client_id}, cleaning data")
                self.clean_data_client(client_id)
            elif start_sync:
                logger.info(f"Received a sync request, sending data fragments (sync_id = {self.get_sync_id(datafragment)})")
                keys = update_data_fragment_step(datafragment.clone()).keys()
                for key in keys:
                    self.force_send(key)
                datafragment.set_sync(False, True)
                self.mom.publish(datafragment, self.info_key)
                logger.info("Data fragments sent, sync response sent")
            elif not end_sync:
                logger.error(f"Unexpected message in info queue: {datafragment}")
            self.mom.ack(tag)

    def run_filter(self, event):
        while not event.is_set():
            try:
                self.mom.consume_with_callback(self.work_queue, self.callback, event)
            except Exception as e:
                logger.error(f"Error in callback: {e}")
                event.set()

    def run(self):
        self.event = Event()
        filter_proccess = Process(target=self.run_filter, args=(self.event,))
        filter_proccess.start()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while not self.exit and filter_proccess.is_alive():
            msg = NODE_TYPE + "." + self.id + "$"
            try:
                sock.sendto(msg.encode(), self.medic_addres)
            except Exception as e:
                logger.error(f"Error sending hartbeat: {e}")
            finally:
                time.sleep(HARTBEAT_INTERVAL)
        filter_proccess.join()
        

def main():
    filter = Filter()
    filter.run()
    if not filter.exit:
        if filter.mom:
            filter.mom.close()
        if filter.log_writer:
            filter.log_writer.close()
        
if __name__ == "__main__":
    main()