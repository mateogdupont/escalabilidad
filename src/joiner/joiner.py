import sys
import os
import signal
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.structs.data_chunk import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step
from dotenv import load_dotenv
import sys
import time
import logging as logger


CATEGORY_FILTER = "CATEGORY"
YEAR_FILTER = "YEAR"
MAX_AMOUNT_OF_FRAGMENTS = 400
TIMEOUT = 50

class Joiner:
    def __init__(self):
        load_dotenv()
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.mom = MOM(consumer_queues)
        self.books_queue = os.environ["BOOKS_QUEUE"]
        self.reviews_queue = os.environ["REVIEWS_QUEUE"]
        self.books_side_tables = {}
        self.side_tables_ended = set()
        self.results = {}
        self._exit = False
        signal.signal(signal.SIGTERM, self.sigterm_handler)
    
    def sigterm_handler(self):
        self._exit = True

    def save_book_in_table(self,book: Book, query_id: str):
        if query_id not in self.books_side_tables.keys():
            self.books_side_tables[query_id] = {}
        self.books_side_tables[query_id][book.get_title()] = book
    
    def process_book_fragment(self,fragment):
        # logger.info(f"Processing book fragment")
        book = fragment.get_book()
        query_id = fragment.get_query_id()
        self.save_book_in_table(book,query_id)
        # logger.info(f"Book fragment processed")

    def receive_all_books(self, query_id: str):
        logger.info(f"Receiving all books for query {query_id}")
        tries = 60*3
        completed = False
        some_books = False
        while not self._exit and not completed:
            msg = self.mom.consume(self.books_queue)
            if not msg:
                tries -= 1 if some_books else 0
                time.sleep(1)
                if tries == 0:
                    logger.info(f"(not last) Finished receiving all books for query {query_id}")
                    self.side_tables_ended.add(query_id)
                    break
                continue
            (data_chunk, tag) = msg
            for fragment in data_chunk.get_fragments():
                if fragment.is_last():
                    f_query_id = fragment.get_query_id()
                    logger.info(f"Finished receiving all books for query {f_query_id}")
                    self.side_tables_ended.add(f_query_id)
                    if f_query_id == query_id:
                        completed = True
                else:
                    self.process_book_fragment(fragment)
                    some_books = True
            self.mom.ack(tag)

    def add_and_try_to_send_chunk(self, fragment: DataFragment, node: str):
        if not node in self.results.keys():
            self.results[node] = ([], time.time())
        self.results[node][0].append(fragment)
        self.results[node] = (self.results[node][0], time.time())
        if len(self.results[node][0]) == MAX_AMOUNT_OF_FRAGMENTS or fragment.is_last():
            data_chunk = DataChunk(self.results[node][0])
            self.mom.publish(data_chunk, node)
            self.results[node] = ([], time.time())

    def process_review_fragment(self, fragment: DataFragment):
        review = fragment.get_review()
        query_id = fragment.get_query_id()
        if review is not None:
            # logger.info(f"self.books_side_tables: {self.books_side_tables}")
            # logger.info(f"query_id: {query_id}")
            side_table = self.books_side_tables[query_id]
            book = side_table.get(review.get_book_title(), None)
            if book is not None:
                fragment.set_book(book)
                for data, key in update_data_fragment_step(fragment).items():
                    self.add_and_try_to_send_chunk(data, key)
        if fragment.is_last():
            for data, key in update_data_fragment_step(fragment).items():
                logger.info(f"Sending to {key}")
                self.add_and_try_to_send_chunk(data, key)

    def send_with_timeout(self):
        for key, (data, last_sent) in self.results.items():
            if (len(data) > 0) and (time.time() - last_sent > TIMEOUT):
                chunk = DataChunk(data)
                self.mom.publish(chunk, key)
                self.results[key] = ([], time.time())

    def run(self):
        # self.receive_all_books()
        while not self._exit:
            msg = self.mom.consume(self.reviews_queue)
            if not msg:
                continue
            (data_chunk, tag) = msg
            for fragment in data_chunk.get_fragments():
                if fragment.get_query_id() not in self.side_tables_ended:
                    self.receive_all_books(fragment.get_query_id())
                self.process_review_fragment(fragment)
            self.mom.ack(tag)
            self.send_with_timeout()

def main():
    joiner = Joiner()
    joiner.run()
   
if __name__ == "__main__":
    main()