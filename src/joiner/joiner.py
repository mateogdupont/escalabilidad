import sys
import os
import signal
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.structs.data_chunk import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step
from dotenv import load_dotenv # type: ignore
import sys
import time
import logging as logger


CATEGORY_FILTER = "CATEGORY"
YEAR_FILTER = "YEAR"
MAX_AMOUNT_OF_FRAGMENTS = 250
TIMEOUT = 50
MAX_WAIT_TIME = 60 * 15

class Joiner:
    def __init__(self):
        load_dotenv()
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        self.id = os.environ["ID"]
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.books_queue = os.environ["BOOKS_QUEUE"] + '.' + self.id
        self.reviews_queue = os.environ["REVIEWS_QUEUE"]
        consumer_queues[self.books_queue] = consumer_queues[os.environ["BOOKS_QUEUE"]]
        consumer_queues.pop(os.environ["BOOKS_QUEUE"])
        self.mom = MOM(consumer_queues)
        self.books_side_tables = {}
        self.books = {}
        self.side_tables_ended = set()
        self.results = {}
        self.exit = False
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)
    
    def sigterm_handler(self, signal,frame):
        self.exit = True
        self.mom.close()

    def save_book_in_table(self, book: Book, query_id: str):
        if query_id not in self.books_side_tables.keys():
            self.books_side_tables[query_id] = set()
        self.books_side_tables[query_id].add(book.get_title())
        if not book.get_title() in self.books.keys():
            self.books[book.get_title()] = book
    
    def process_book_fragment(self,fragment):
        book = fragment.get_book()
        query_id = fragment.get_query_id()
        self.save_book_in_table(book,query_id)

    def receive_all_books(self, query_id=None):
        logger.info(f"Receiving all books for query {query_id}")
        completed = False
        while not self.exit and not completed:
            msg = self.mom.consume(self.books_queue)
            if not msg:
                time.sleep(1)
                # if time.time() - time_now > MAX_WAIT_TIME:
                #     logger.info(f"(Not last) Finished receiving all books for query {query_id}")
                #     self.side_tables_ended.add(query_id)
                #     break
                continue
            (data_chunk, tag) = msg
            for fragment in data_chunk.get_fragments():
                if self.exit:
                    return
                f_query_id = fragment.get_query_id()
                if fragment.is_last():
                    logger.info(f"Finished receiving all books for query {f_query_id}")
                    self.side_tables_ended.add(f_query_id)
                    if query_id is None or f_query_id == query_id:
                        completed = True
                else:
                    self.process_book_fragment(fragment)
            self.mom.ack(tag)

    def add_and_try_to_send_chunk(self, fragment: DataFragment, node: str):
        if self.exit:
            return
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
        if (not fragment.is_last()) and (review is not None) and (not self.exit):
            side_table = self.books_side_tables[query_id]
            book_name = side_table.get(review.get_book_title(), None)
            book = None
            if book_name is not None:
                book = self.books[book_name]
            if book is not None:
                fragment.set_book(book)
                for data, key in update_data_fragment_step(fragment).items():
                    self.add_and_try_to_send_chunk(data, key)
        if (fragment.is_last()) and (not self.exit):
            for data, key in update_data_fragment_step(fragment).items():
                logger.info(f"Sending to {key}")
                self.add_and_try_to_send_chunk(data, key)

    def send_with_timeout(self):
        for key, (data, last_sent) in self.results.items():
            if self.exit:
                return
            if (len(data) > 0) and (time.time() - last_sent > TIMEOUT):
                chunk = DataChunk(data)
                self.mom.publish(chunk, key)
                self.results[key] = ([], time.time())

    def run(self):
        self.receive_all_books()
        while not self.exit:
            msg = self.mom.consume(self.reviews_queue)
            if not msg:
                time.sleep(0.1)
                continue
            (data_chunk, tag) = msg
            ack = True
            for fragment in data_chunk.get_fragments():
                if self.exit:
                    return
                if fragment.get_query_id() not in self.side_tables_ended:
                    ack = False
                    self.mom.nack(tag)
                    self.receive_all_books(fragment.get_query_id())
                    break
                self.process_review_fragment(fragment)
            if ack:
                self.mom.ack(tag)
            self.send_with_timeout()

def main():
    joiner = Joiner()
    joiner.run()
    if not joiner.exit:
        joiner.mom.close()
   
if __name__ == "__main__":
    main()