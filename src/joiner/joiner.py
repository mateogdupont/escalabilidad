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
MAX_AMOUNT_OF_FRAGMENTS = 800
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
        self.side_tables_ended = {}
        self.received_ids = {}
        self.results = {}
        self.exit = False
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)
    
    def sigterm_handler(self, signal,frame):
        self.exit = True
        self.mom.close()

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

    def clean_data(self, query_id: str, client_id: str):
        if client_id in self.books_side_tables.keys():
            if query_id in self.books_side_tables[client_id].keys():
                self.books_side_tables[client_id].pop(query_id)
            if len(self.books_side_tables[client_id]) == 0:
                self.books_side_tables.pop(client_id)
        if client_id in self.side_tables_ended.keys():
            if query_id in self.side_tables_ended[client_id]:
                self.side_tables_ended[client_id].remove(query_id)
            if len(self.side_tables_ended[client_id]) == 0:
                self.side_tables_ended.pop(client_id)
    
    def is_side_table_ended(self, query_id: str, client_id: str,):
        if not client_id in self.side_tables_ended.keys():
            return False
        return query_id in self.side_tables_ended[client_id]

    def save_book_in_table(self, book: Book, query_id: str, client_id: str):
        if client_id not in self.books_side_tables.keys():
            self.books_side_tables[client_id] = {}
        if query_id not in self.books_side_tables[client_id].keys():
            self.books_side_tables[client_id][query_id] = set()
        self.books_side_tables[client_id][query_id].add(book.get_title())
        if not book.get_title() in self.books.keys():
            self.books[book.get_title()] = book
    
    def process_book_fragment(self,fragment):
        book = fragment.get_book()
        query_id = fragment.get_query_id()
        client_id = fragment.get_client_id()
        self.save_book_in_table(book, query_id, client_id)

    def receive_all_books(self, query_id=None, client_id=None):
        logger.info(f"Receiving all books for query {query_id}")
        completed = False
        while not self.exit and not completed:
            msg = self.mom.consume(self.books_queue)
            if not msg:
                continue
            (data_chunk, tag) = msg
            for fragment in data_chunk.get_fragments():
                if self.exit:
                    return
                if not self.save_id(fragment):
                    continue
                f_query_id = fragment.get_query_id()
                f_client_id = fragment.get_client_id()
                if fragment.is_last():
                    logger.info(f"Finished receiving all books for query {f_query_id}")
                    if not f_client_id in self.side_tables_ended.keys():
                        self.side_tables_ended[f_client_id] = set()
                    self.side_tables_ended[f_client_id].add(f_query_id)
                    wanted_client = client_id is None or f_client_id == client_id
                    wanted_query = query_id is None or f_query_id == query_id
                    if wanted_client and wanted_query:
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
        client_id = fragment.get_client_id()
        if (not fragment.is_last()) and (review is not None) and (not self.exit):
            side_table = self.books_side_tables[client_id][query_id]
            if review.get_book_title() in side_table:
                book = self.books[review.get_book_title()]
                fragment.set_book(book)
                for data, key in update_data_fragment_step(fragment).items():
                    self.add_and_try_to_send_chunk(data, key)
        if (fragment.is_last()) and (not self.exit):
            for data, key in update_data_fragment_step(fragment).items():
                logger.info(f"Sending to {key}")
                self.add_and_try_to_send_chunk(data, key)
            self.clean_data(query_id, client_id)

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
                continue
            (data_chunk, tag) = msg
            ack = True
            for fragment in data_chunk.get_fragments():
                if self.exit:
                    return
                if not self.is_side_table_ended(fragment.get_query_id(), fragment.get_client_id()):
                    ack = False
                    self.mom.nack(tag)
                    self.receive_all_books(fragment.get_query_id(), fragment.get_client_id())
                    break
                if not self.save_id(fragment):
                    continue
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