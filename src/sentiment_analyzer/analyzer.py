import signal
import os
import socket
from utils.structs.data_chunk import *
from utils.mom.mom import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.query_updater import *
from log_manager.log_writer import *
from multiprocessing import Process, Event
from dotenv import load_dotenv # type: ignore
from threading import Thread
import logging as logger
import sys
import os
import time
from textblob import TextBlob # type: ignore
from log_manager.log_recoverer import *

def get_sentiment_score(text: str) -> float:
    return round(TextBlob(text).sentiment.polarity, 5)

load_dotenv()
NODE_TYPE=os.environ["NODE_TYPE"]
HARTBEAT_INTERVAL=int(os.environ["HARTBEAT_INTERVAL"])
CATEGORY_FILTER = "CATEGORY"
YEAR_FILTER = "YEAR"
TITLE_FILTER = "TITLE"
DISTINCT_FILTER = "COUNT_DISTINCT"
SENTIMENT_FILTER = "SENTIMENT"
MAX_AMOUNT_OF_FRAGMENTS = 800
TIMEOUT = 50

MAX_SLEEP = 10 # seconds
MULTIPLIER = 0.1

class Analyzer:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        log_recoverer = LogRecoverer(os.environ["LOG_PATH"])
        log_recoverer.recover_data()
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.work_queue = consumer_queues[0]
        self.info_queue = os.environ["INFO_QUEUE"]
        self.info_key = os.environ["INFO_KEY"]
        self.nodes = int(os.environ["SENTIMENT_NODES"])
        consumer_queues.append(self.info_queue)
        self.mom = MOM(consumer_queues)
        self.results = log_recoverer.get_results()
        self.received_ids = log_recoverer.get_received_ids()
        self.ignore_ids = log_recoverer.get_ignore_ids()
        self.medic_addres = (os.environ["MEDIC_IP"], int(os.environ["MEDIC_PORT"]))
        self.id= os.environ["ID"]
        self.event = None
        self.exit = False
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)
        self.log_writer = LogWriter(os.environ["LOG_PATH"])
    
    def sigterm_handler(self, signal,frame):
        self.exit = True
        if self.mom:
            self.mom.close()
        if self.log_writer:
            self.log_writer.close()
        if self.event:
            self.event.set()

    def clean_data_client(self, client_id):
        logger.info(f"Cleaning data from client {client_id}")
        if client_id in self.received_ids.keys():
            self.received_ids.pop(client_id)
        for node, batch in self.results.items():
            batch = ([fragment for fragment in batch[0] if fragment.get_client_id() != client_id], batch[1])
            self.results[node] = batch
        self.ignore_ids.add(client_id)
        self.log_writer.log_ignore(client_id)
    
    def save_id(self, data_fragment: DataFragment) -> bool:
        client_id = data_fragment.get_client_id()
        query_id = data_fragment.get_query_id()
        id = data_fragment.get_id()
        if client_id in self.ignore_ids:
            return False
        self.received_ids[client_id] = self.received_ids.get(client_id, {})
        self.received_ids[client_id][query_id] = self.received_ids[client_id].get(query_id, set())
        if id in self.received_ids[client_id][query_id]:
            return False
        self.received_ids[client_id][query_id].add(id)
        return True

    def add_and_try_to_send_chunk(self, fragment: DataFragment, node: str, event):
        if event.is_set():
            return
        if not node in self.results.keys():
            self.results[node] = []
        self.results[node].append(fragment)
        if len(self.results[node]) == MAX_AMOUNT_OF_FRAGMENTS or fragment.is_last():
            data_chunk = DataChunk(self.results[node])
            self.mom.publish(data_chunk, node)
            self.log_writer.log_result_sent(node)
            self.results[node] = []

    def process_msg(self, event) -> bool:
        msg = self.mom.consume(self.work_queue)
        if not msg:
            return False
        data_chunk, tag = msg
        for data_fragment in data_chunk.get_fragments():
            if not self.save_id(data_fragment):
                continue
            if (not data_fragment.is_last()) and (not event.is_set()):
                review_text = data_fragment.get_review().get_text()
                sentiment_score = get_sentiment_score(review_text)
                query_info = data_fragment.get_query_info()
                query_info.set_sentiment(sentiment_score)
                data_fragment.set_query_info(query_info)
            elif not event.is_set():
                self.sync_last(data_fragment)
                self.log_writer.log_query_ended(data_fragment)

            # the text is no longer needed
            review = data_fragment.get_review()
            review.set_text("")
            data_fragment.set_review(review)

            next_steps = update_data_fragment_step(data_fragment)

            if not data_fragment.is_last():
                list_next_steps = [(fragment, key) for fragment, key in next_steps.items()]
                self.log_writer.log_result(list_next_steps)

            for fragment, key in next_steps.items():
                self.add_and_try_to_send_chunk(fragment, key, event)
        self.mom.ack(delivery_tag=tag)
        return True

    def sync_last(self, last_data_fragment: DataFragment) -> None:
        logger.info("I have the last, before send it I will sync")
        sync_fragment = last_data_fragment.clone()
        sync_fragment.set_sync(True, False)
        self.mom.publish(sync_fragment, self.info_key)
        logger.info("Sync sent")
        client_id = last_data_fragment.get_client_id()
        query_id = last_data_fragment.get_query_id()
        nodes_left = self.nodes
        last_ack = time.time()
        while nodes_left > 0:
            msg = self.mom.consume(self.info_queue)
            if not msg and time.time() - last_ack < TIMEOUT:
                time.sleep(0.5)
                continue
            elif not msg:
                logger.warning("Timeout waiting for sync response, sending last fragment")
                break
            datafragment, tag = msg
            if datafragment.get_query_info().is_clean_flag():
                self.mom.nack(tag, True)
                logger.info("Received a clean flag, postponing (nack sent)")
                time.sleep(0.5)
                continue
            start_sync, end_sync = datafragment.get_sync()
            if end_sync and datafragment.get_client_id() == client_id and datafragment.get_query_id() == query_id:
                nodes_left -= 1
                logger.info(f"Sync response received, {nodes_left} nodes left")
            elif start_sync:
                if datafragment.get_client_id() == client_id and datafragment.get_query_id() == query_id:
                    nodes_left -= 1
                else:
                    logger.info("Received a sync request, sending data fragments")
                    self.send_all()
                    datafragment.set_sync(False, True)
                    self.mom.publish(datafragment, self.info_key)
                    logger.info("Data fragments sent, sync response sent")
            self.mom.ack(tag)
            last_ack = time.time()

        logger.info("All nodes synced, ready to send last fragment")

    def send_all(self):
        for key, (data, _) in self.results.items():
            if len(data) > 0:
                chunk = DataChunk(data)
                try:
                    self.mom.publish(chunk, key)
                    self.log_writer.log_result_sent(key)
                except Exception as e:
                    logger.error(f"Error al enviar a {key}: {e}")
                    logger.error(f"Data: {chunk.to_bytes()}")
                self.results[key] = ([], time.time())

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
                self.send_all()
                datafragment.set_sync(False, True)
                self.mom.publish(datafragment, self.info_key)
            elif not end_sync:
                logger.error(f"Unexpected message in info queue: {datafragment}")
            self.mom.ack(tag)

    def run_analizer(self, event):
        times_empty = 0
        while not event.is_set():
            try:
                self.inspect_info_queue(event)
                if not self.process_msg(event):
                    times_empty += 1
                    time.sleep(min(MAX_SLEEP, (times_empty**2) * MULTIPLIER))
                    continue
                times_empty = 0
            except Exception as e:
                logger.error(f"Error in analyzer: {e}")
                event.set()
    
    def send_with_timeout(self, event):
        for node, batch in self.results.items():
            if len(batch) > 0:
                data_chunk = DataChunk(batch)
                try:
                    self.mom.publish(data_chunk, node)
                    self.log_writer.log_result_sent(node)
                except Exception as e:
                    logger.error(f"Error al enviar a {node}: {e}")
                    logger.error(f"Data: {data_chunk.to_bytes()}")
                self.results[node] = []

    def run(self):
        self.event = Event()
        analyzer_proccess = Process(target=self.run_analizer, args=(self.event,))
        analyzer_proccess.start()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while not self.exit and analyzer_proccess.is_alive():
            msg = NODE_TYPE + "." + self.id + "$"
            try:
                sock.sendto(msg.encode(), self.medic_addres)
            except Exception as e:
                logger.error(f"Error sending hartbeat: {e}")
            finally:
                time.sleep(HARTBEAT_INTERVAL)
        analyzer_proccess.join()

def main() -> None:
    analyzer = Analyzer()
    analyzer.run()
    if not analyzer.exit:
        if analyzer.mom:
            analyzer.mom.close()
        if analyzer.log_writer:
            analyzer.log_writer.close()

if __name__ == "__main__":
    main()