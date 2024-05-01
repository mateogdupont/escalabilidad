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
MAX_AMOUNT_OF_FRAGMENTS = 100
TIMEOUT = 10

class Joiner:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        load_dotenv()
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.mom = MOM(consumer_queues)
        self.books_queue = os.environ["BOOKS_QUEUE"]
        self.reviews_queue = os.environ["REVIEWS_QUEUE"]
        self.nineties_books_side_table = {}
        self.fiction_books_side_table = {}
        self.results = {}
        self._exit = False
        signal.signal(signal.SIGTERM, self.sigterm_handler)
    
    def sigterm_handler(self):
        self._exit = True

    def save_book_in_table(self,book: Book, filter: str):
        if filter == YEAR_FILTER:
            self.books_side_table[book.get_title] = book
        elif filter == CATEGORY_FILTER:
            self.books_side_table[book.get_title] = book
        else:
            logger.info(f"Error, book with unknown filter: {filter}")
    
    def process_book_fragment(self,fragment):
        book = fragment.get_book()
        query_info = fragment.query_info()
        if query_info is not None:
            filter_on, word, min_value, max_value = query_info.get_filter_params()
            if book is not None:
                self.save_book_in_table(book,filter_on)

    def receive_all_books(self):
        complete = False
        while not self._exit and not complete:
            (data_chunk, tag) = self.mom.consume(self.books_queue)
            if not data_chunk:
                continue
            for fragment in data_chunk.get_fragments():
                self.process_book_fragment(fragment)
                if fragment.is_last():
                    complete = True
            self.mom.ack(tag)

    def add_and_try_to_send_chunk(self, fragment: DataFragment, node: str):
        if not node in self.results.keys():
            self.results[node] = ([], time.time())
        self.results[node][0].append(fragment)
        self.results[node] = (self.results[node][0], time.time())
        if len(self.results[node][0]) == MAX_AMOUNT_OF_FRAGMENTS:
            data_chunk = DataChunk(self.results[node][0])
            self.mom.publish(data_chunk, node)
            self.results[node] = ([], time.time())

    def process_review_fragment(self, fragment: DataFragment):
        review = fragment.get_review()
        if review is not None:
            book = self.books_side_table[review.get_book_title()]
            if book is not None:
                fragment.set_book(book)
                for data, key in update_data_fragment_step(fragment).items():
                    self.add_and_try_to_send_chunk(data, key)
                
                #if fragment.is_last():
                #    self.update_last_and_send_chunk()
            else: 
                logger.info(f"Error: Book not found: {review.get_book_title()}")

    def send_with_timeout(self):
        for key, (data, last_sent) in self.results.items():
            if (len(data) > 0) and (time.time() - last_sent > TIMEOUT):
                chunk = DataChunk(data)
                self.mom.publish(chunk, key)
                self.results[key] = ([], time.time())

    def run(self):
        self.receive_all_books()
        while not self._exit:
            (data_chunk, tag) = self.mom.consume(self.reviews_queue)
            for fragment in data_chunk.fragments():
                self.process_review_fragment(fragment)
            self.mom.ack(tag)
            self.send_with_timeout()

def main():
    joiner = Joiner()
    joiner.run()
   
if __name__ == "__main__":
    main()