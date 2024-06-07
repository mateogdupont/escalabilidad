import signal
import os
import time
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.structs.data_chunk import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step
from dotenv import load_dotenv # type: ignore
import logging as logger
import sys
from filter.log_writer import *

CATEGORY_FILTER = "CATEGORY"
YEAR_FILTER = "YEAR"
TITLE_FILTER = "TITLE"
DISTINCT_FILTER = "COUNT_DISTINCT"
SENTIMENT_FILTER = "SENTIMENT"
MAX_AMOUNT_OF_FRAGMENTS = 800
TIMEOUT = 50
TOP_AMOUNT = 10

LOG_PATH = "logs/filter.log"

class Filter:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        load_dotenv()
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.work_queue = list(consumer_queues.keys())[0]
        self.mom = MOM(consumer_queues)
        self.results = {}
        self.received_ids = {}
        self.top_ten = {}
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)
        self.exit = False
        self.log_writer = LogWriter(LOG_PATH)
    
    def sigterm_handler(self, signal,frame):
        self.exit = True
        self.mom.close()

    def save_id(self, data_fragment: DataFragment) -> bool:
        client_id = data_fragment.get_client_id() # TODO: review with feat-multiclient branch
        query_id = data_fragment.get_query_id()
        id = data_fragment.get_id()
        self.received_ids[client_id] = self.received_ids.get(client_id, {})
        self.received_ids[client_id][query_id] = self.received_ids[client_id].get(query_id, set())
        if id in self.received_ids[client_id][query_id]:
            logger.warning("-----------------------------------------------")
            logger.warning(f"Repeated id: {id} from client: {client_id} query: {query_id}")
            logger.warning(f"Data saved: {self.received_ids[client_id][query_id][id]}")
            logger.warning(f"Data received: {data_fragment.to_human_readable()}")
            logger.warning("-----------------------------------------------")
            return False
        self.received_ids[client_id][query_id].add(id)
        return True

    def filter_data_fragment(self, data_fragment: DataFragment) -> bool:
        query_info = data_fragment.get_query_info()
        filter_on, word, min_value, max_value = query_info.get_filter_params()
        book = data_fragment.get_book()
        client_id = data_fragment.get_client_id()
        query_id = data_fragment.get_query_id()

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
            self.top_ten[client_id] = self.top_ten.get(client_id, {})
            self.top_ten[client_id][query_id] = self.top_ten[client_id].get(query_id, [])
            if data_fragment.is_last():
                #logger.info(f"Me llego el ultimo fragmento {data_fragment.to_json()}")
                for fragment in self.top_ten[client_id][query_id]:
                    if self.exit:
                        return False
                    for data, key in update_data_fragment_step(fragment).items():
                        self.add_and_try_to_send_chunk(data, key)
                # delete query from client
                self.top_ten[client_id].pop(query_id)
                if len(self.top_ten[client_id].keys()) == 0:
                    self.top_ten.pop(client_id)

            if len(self.top_ten[client_id][query_id]) < TOP_AMOUNT:
                #logger.info(f"Fragmento entro al top 10 cuando hay: {len(self.top_ten)} con av: {data_fragment.get_query_info().get_average()}")
                self.top_ten[client_id][query_id].append(data_fragment)
            else:
                lowest = self.top_ten[client_id][query_id][0]
                if data_fragment.get_query_info().get_average() > lowest.get_query_info().get_average():
                    #logger.info(f"Fragmento entro al top 10 cuando hay: {len(self.top_ten)} con av: {data_fragment.get_query_info().get_average()}")
                    self.top_ten[client_id][query_id][0] = data_fragment
            self.top_ten[client_id][query_id] = sorted(self.top_ten[client_id][query_id], key=lambda fragment: fragment.get_query_info().get_average())
        
        return False

    def add_and_try_to_send_chunk(self, fragment: DataFragment, node: str):
        if self.exit:
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
                logger.error(f"Data: {data_chunk.to_str()}")
            self.results[node] = ([], time.time())
        
    def filter_data_chunk(self,chunk: DataChunk):
        for fragment in chunk.get_fragments():
            if self.exit:
                return
            if not self.save_id(fragment):
                continue
            if not fragment.is_last():
                if self.filter_data_fragment(fragment):
                    next_steps = update_data_fragment_step(fragment)
                    if len(next_steps.items()) == 0:
                        logger.info(f"Fragmento {fragment} no tiene siguiente paso")
                    list_next_steps = [(fragment, key) for fragment, key in next_steps.items()]
                    self.log_writer.log_result(list_next_steps)
                    for data, key in next_steps.items():
                        self.add_and_try_to_send_chunk(data, key)
                elif fragment.get_query_info().filter_by_top():
                    client_id = fragment.get_client_id()
                    query_id = fragment.get_query_id()
                    top_ten = self.top_ten.get(client_id, {}).get(query_id, [])
                    self.log_writer.log_top_update(fragment, top_ten)
                else:
                    self.log_writer.log_received_id(fragment)
            if fragment.is_last():
                self.log_writer.log_query_ended(fragment)
                if fragment.get_query_info().filter_by_top():
                    return self.send_top(fragment)
                    
                next_steps = update_data_fragment_step(fragment)
                for data, key in next_steps.items():
                    self.add_and_try_to_send_chunk(data, key)

    def send_top(self, fragment): # TODO: Manejar log
        client_id = fragment.get_client_id()
        query_id = fragment.get_query_id()
        self.top_ten[client_id] = self.top_ten.get(client_id, {})
        self.top_ten[client_id][query_id] = self.top_ten[client_id].get(query_id, [])
        for top_fragment in self.top_ten[client_id][query_id]:
            if self.exit:
                return False
            for data, key in update_data_fragment_step(top_fragment).items():
                self.add_and_try_to_send_chunk(data, key)
        # self.top_ten = []

    def send_with_timeout(self):
        for key, (data, last_sent) in self.results.items():
            if self.exit:
                return
            if (len(data) > 0) and (time.time() - last_sent > TIMEOUT):
                chunk = DataChunk(data)
                try:
                    self.mom.publish(chunk, key)
                    self.log_writer.log_result_sent(node)
                except Exception as e:
                    logger.error(f"Error al enviar a {key}: {e}")
                    logger.error(f"Data: {chunk.to_str()}")
                self.results[key] = ([], time.time())

    def run(self):
        while not self.exit:
            try:
                msg = self.mom.consume(self.work_queue)
            except Exception as e:
                logger.error(f"Error al consumir de {self.work_queue}: {e}")
                return
              
            if not msg:
                self.send_with_timeout()
                continue
            data_chunk, tag = msg
            # logger.info(f"Recibi data | {data_chunk.to_json()}")
            self.filter_data_chunk(data_chunk)
            try:
                self.mom.ack(tag)
            except Exception as e:
                logger.error(f"Error al hacer ack de {tag}: {e}")
            self.send_with_timeout()

def main():
    filter = Filter()
    filter.run()
    if not filter.exit:
        filter.mom.close()
        
if __name__ == "__main__":
    main()