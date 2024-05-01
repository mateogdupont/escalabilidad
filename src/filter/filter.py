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

CATEGORY_FILTER = "CATEGORY"
YEAR_FILTER = "YEAR"
TITLE_FILTER = "TITLE"
DISTINCT_FILTER = "COUNT_DISTINCT"
SENTIMENT_FILTER = "SENTIMENT"
MAX_AMOUNT_OF_FRAGMENTS = 1
TIMEOUT = 10

class Filter:
    def __init__(self):
        load_dotenv()
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.work_queue = list(consumer_queues.keys())[0]
        self.mom = MOM(consumer_queues)
        self.results = dict.fromkeys(['results', 'filter', 'counter', 'joiner_books', 'joiner_reviews', 'sentiment_analysis'], ([], time.time()))
        self.top_ten = []
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        self._exit = False
    
    def sigterm_handler(self):
        self._exit = True

    def filter_data_fragment(self, data_fragment: DataFragment) -> bool:
        logger.info("por evaluar")
        query_info = data_fragment.get_query_info()
        filter_on, word, min_value, max_value = query_info.get_filter_params()
        book = data_fragment.get_book()
        query_info = data_fragment.get_query_info()
        logger.info("por serie de ifs")

        if (filter_on == CATEGORY_FILTER) and (book is not None):
            logger.info(f"Entro al filtro por categoria y da: {word in book.get_categories()}")
            return word in book.get_categories()
        elif (filter_on == YEAR_FILTER) and (book is not None):
            book_year = book.get_published_year()
            logger.info(f"Entro al filtro por año y da: {min_value < book_year < max_value}")
            return min_value < book_year < max_value
        elif (filter_on == TITLE_FILTER) and (book is not None):
            logger.info(f"Entro al filtro por titulo y da: {word in book.get_title()}")
            return word in book.get_title()
        elif filter_on == DISTINCT_FILTER:
            logger.info(f"Entro al filtro por distinct")
            return query_info.get_n_distinct() >= min_value
        elif filter_on == SENTIMENT_FILTER:
            logger.info(f"Entro al filtro por sentiment")
            return query_info.get_sentiment() >= min_value
        else:
            #TODO: Apply filter of top10
            logger.info(f"No entre a nadapor que filter on es: {filter_on}")
            return False
            if data_fragment.is_last():
                #Send all data
                self.top_ten = []
                return False
            #Check if is a top 10 and insert it in order
        return False
    
    def add_and_try_to_send_chunk(self,fragment: DataFragment, key: str):
        if fragment.is_last():
            for key, (data, last_sent) in self.results.items():
                chunk = DataChunk(data)
                self.mom.publish(chunk, key)
                self.results[key] = ([], time.time())
        else:
            self.results[key][0].append(fragment)
            if len(self.results[key][0]) == MAX_AMOUNT_OF_FRAGMENTS or fragment.is_last():
                chunk = DataChunk(self.results[key][0])
                self.mom.publish(chunk, key)
                self.results[key] = ([], time.time())
        


    def filter_data_chunk(self,chunk: DataChunk):
        logger.info("Por entrar al primer for")
        for fragment in chunk.get_fragments():
            logger.info("Por evaluar la cond")
            if self.filter_data_fragment(fragment):
                logger.info("Despues de en true")
                for data, key in update_data_fragment_step(fragment).items():
                    self.add_and_try_to_send_chunk(data, key)
                self.add_and_try_to_send_chunk(data, key)
            logger.info("Despues de evaluar")

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
            logger.info(f"Recibi {msg}")
            data_chunk, tag = msg
            self.filter_data_chunk(data_chunk)
            self.mom.ack(tag)
            self.send_with_timeout()

def main():
    filter = Filter()
    filter.run()
   
if __name__ == "__main__":
    main()