import signal
import os
import time
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.structs.data_chunk import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step
from dotenv import load_dotenv
import logging as logger
import sys

CATEGORY_FILTER = "CATEGORY"
YEAR_FILTER = "YEAR"
TITLE_FILTER = "TITLE"
DISTINCT_FILTER = "COUNT_DISTINCT"
SENTIMENT_FILTER = "SENTIMENT"
MAX_AMOUNT_OF_FRAGMENTS = 100
TIMEOUT = 10
TOP_AMOUNT = 10

class Filter:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        load_dotenv()
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.work_queue = list(consumer_queues.keys())[0]
        self.mom = MOM(consumer_queues)
        self.results = {}
        self.top_ten = []
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        self._exit = False
    
    def sigterm_handler(self):
        self._exit = True

    def filter_data_fragment(self, data_fragment: DataFragment) -> bool:
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
            return query_info.get_n_distinct() >= 1 #min_value
        elif filter_on == SENTIMENT_FILTER and (query_info.get_sentiment() is not None):
            return query_info.get_sentiment() >= min_value
        elif query_info.filter_by_top():
            logger.info(f"Top ten: {self.top_ten} | len {len(self.top_ten)}")
            if len(self.top_ten) < TOP_AMOUNT:
                self.top_ten.append(data_fragment)
                self.top_ten = sorted(self.top_ten, key=lambda fragment: fragment.get_query_info().get_average())
            else:
                lowest = self.top_ten[0]
                if data_fragment.get_query_info().get_average() > lowest.get_query_info().get_average():
                    self.top_ten[0] = data_fragment
                    self.top_ten = sorted(self.top_ten, key=lambda fragment: fragment.get_query_info().get_average())
        
            if data_fragment.is_last():
                logger.info("data_fragment is last")
                for fragment in self.top_ten:
                    for data, key in update_data_fragment_step(fragment).items():
                        self.add_and_try_to_send_chunk(data, key)
                self.top_ten = []
        return False

    
    def add_and_try_to_send_chunk(self, fragment: DataFragment, node: str):
        if not node in self.results.keys():
            self.results[node] = ([], time.time())
        self.results[node][0].append(fragment)
        if len(self.results[node][0]) == MAX_AMOUNT_OF_FRAGMENTS or fragment.is_last():
            data_chunk = DataChunk(self.results[node][0])
            self.mom.publish(data_chunk, node)
            self.results[node] = ([], time.time())

    # def update_last_and_send_chunk(self, queries_to_update):
    #     for query in queries_to_update:
    #         for key, (data, last_sent) in self.results.items():
    #             for i, fragment in enumerate(reversed(data)):
    #                 queries = fragment.get_queries()
    #                 if query in queries.keys():
    #                     fragment.set_as_last()
    #                     data[i] = fragment
    #                     break
    #             self.results[key] = (data,last_sent)
        
    def filter_data_chunk(self,chunk: DataChunk):
        for fragment in chunk.get_fragments():
            if self.filter_data_fragment(fragment):
                for data, key in update_data_fragment_step(fragment).items():
                    self.add_and_try_to_send_chunk(data, key)
            if fragment.is_last():
                next_steps = update_data_fragment_step(fragment)
                for data, key in next_steps.items():
                    self.add_and_try_to_send_chunk(data, key)

    def send_with_timeout(self):
        for key, (data, last_sent) in self.results.items():
            if (len(data) > 0) and (time.time() - last_sent > TIMEOUT):
                chunk = DataChunk(data)
                self.mom.publish(chunk, key)
                self.results[key] = ([], time.time())


    def run(self):
        while not self._exit:
            msg = self.mom.consume(self.work_queue)
            if not msg:
                continue
                #return # TODO: change this
            # logger.info(f"Recibi {msg}")
            data_chunk, tag = msg
            self.filter_data_chunk(data_chunk)
            self.mom.ack(tag)
            self.send_with_timeout()

def main():
    filter = Filter()
    filter.run()
   
if __name__ == "__main__":
    main()