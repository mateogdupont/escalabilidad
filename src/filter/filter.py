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
TOP_AMOUNT = 10

class Filter:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.work_queue = list(consumer_queues.keys())[0]
        self.mom = MOM(consumer_queues)
        self.results = {}
        self.received_ids = {}
        self.top_ten = []
        self.event = None
        self.medic_addres = (os.environ["MEDIC_IP"], int(os.environ["MEDIC_PORT"]))
        self.id= os.environ["ID"]
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)
        self.exit = False
    
    def sigterm_handler(self, signal,frame):
        self.exit = True
        self.mom.close()
        if self.event:
            self.event.set()

    def save_id(self, data_fragment: DataFragment) -> bool:
        client_id = data_fragment.get_client_id() # TODO: review with feat-multiclient branch
        query_id = data_fragment.get_query_id()
        id = data_fragment.get_id()
        self.received_ids[client_id] = self.received_ids.get(client_id, {})
        self.received_ids[client_id][query_id] = self.received_ids[client_id].get(query_id, {})
        if id in self.received_ids[client_id][query_id]:
            logger.warning("-----------------------------------------------")
            logger.warning(f"Repeated id: {id} from client: {client_id} query: {query_id}")
            logger.warning(f"Data saved: {self.received_ids[client_id][query_id][id]}")
            logger.warning(f"Data received: {data_fragment.to_human_readable()}")
            logger.warning("-----------------------------------------------")
            return False
        self.received_ids[client_id][query_id][id] = data_fragment.to_human_readable()
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
        elif query_info.filter_by_top():
            if data_fragment.is_last():
                #logger.info(f"Me llego el ultimo fragmento {data_fragment.to_json()}")
                for fragment in self.top_ten:
                    if event.is_set():
                        return False
                    for data, key in update_data_fragment_step(fragment).items():
                        self.add_and_try_to_send_chunk(data, key)
                self.top_ten = []

            if len(self.top_ten) < TOP_AMOUNT:
                #logger.info(f"Fragmento entro al top 10 cuando hay: {len(self.top_ten)} con av: {data_fragment.get_query_info().get_average()}")
                self.top_ten.append(data_fragment)
                self.top_ten = sorted(self.top_ten, key=lambda fragment: fragment.get_query_info().get_average())
            else:
                lowest = self.top_ten[0]
                if data_fragment.get_query_info().get_average() > lowest.get_query_info().get_average():
                    #logger.info(f"Fragmento entro al top 10 cuando hay: {len(self.top_ten)} con av: {data_fragment.get_query_info().get_average()}")
                    self.top_ten[0] = data_fragment
                    self.top_ten = sorted(self.top_ten, key=lambda fragment: fragment.get_query_info().get_average())
        
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
            except Exception as e:
                logger.error(f"Error al enviar a {node}: {e}")
                logger.error(f"Data: {data_chunk.to_str()}")
            self.results[node] = ([], time.time())
        
    def filter_data_chunk(self,chunk: DataChunk, event):
        for fragment in chunk.get_fragments():
            if event.is_set():
                return
            if not self.save_id(fragment):
                continue
            if (not fragment.is_last()) and self.filter_data_fragment(fragment,event):
                next_steps = update_data_fragment_step(fragment)
                if len(next_steps.items()) == 0:
                    logger.info(f"Fragmento {fragment} no tiene siguiente paso")
                for data, key in next_steps.items():
                    self.add_and_try_to_send_chunk(data, key, event)
            if fragment.is_last():
                if fragment.get_query_info().filter_by_top():
                    for top_fragment in self.top_ten:
                        if event.is_set():
                            return False
                        for data, key in update_data_fragment_step(top_fragment).items():
                            self.add_and_try_to_send_chunk(data, key, event)
                    self.top_ten = []
                next_steps = update_data_fragment_step(fragment)
                for data, key in next_steps.items():
                    self.add_and_try_to_send_chunk(data, key, event)

    def send_with_timeout(self,event):
        for key, (data, last_sent) in self.results.items():
            if event.is_set():
                return
            if (len(data) > 0) and (time.time() - last_sent > TIMEOUT):
                chunk = DataChunk(data)
                try:
                    self.mom.publish(chunk, key)
                except Exception as e:
                    logger.error(f"Error al enviar a {key}: {e}")
                    logger.error(f"Data: {chunk.to_str()}")
                self.results[key] = ([], time.time())

    def callback(self, ch, method, properties, body,event):
        try:
            data_chunk = DataChunk.from_str(body)
            self.filter_data_chunk(data_chunk,event)
            self.mom.ack(delivery_tag=method.delivery_tag)
            self.send_with_timeout(event)
        except Exception as e:
            logger.error(f"Error en callback: {e}")

    def run_filter(self, event):
        while not event.is_set():
            self.mom.consume_with_callback(self.work_queue, self.callback, event)

    def run(self):
        self.event = Event()
        filter_proccess = Process(target=self.run_filter, args=(self.event,))
        filter_proccess.start()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while not self.exit and filter_proccess.is_alive():
            msg = NODE_TYPE + "." + self.id + "$"
            sock.sendto(msg.encode(), self.medic_addres)
            time.sleep(HARTBEAT_INTERVAL)
        filter_proccess.join()
        

def main():
    filter = Filter()
    filter.run()
    if not filter.exit:
        filter.mom.close()
        
if __name__ == "__main__":
    main()