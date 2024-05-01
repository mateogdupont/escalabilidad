import sys
import os
import signal
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step
from dotenv import load_dotenv
import sys
import logging as logger

class Joiner:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        load_dotenv()
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.mom = MOM(consumer_queues)
        self.books_queue = os.environ["BOOKS_QUEUE"]
        self.reviews_queue = os.environ["REVIEWS_QUEUE"]
        # self.mom = MOM({'filter':None,'joiner_books':None,'joiner_reviews':None,'results':None})
        self.books_side_table = {}
        self._exit = False
        signal.signal(signal.SIGTERM, self.sigterm_handler)
    
    def sigterm_handler(self):
        self._exit = True

    #TODO: Add stop condition when finish reading books
    def receive_all_books(self):
        while not self._exit:
            (data_fragment, tag) = self.mom.consume(self.books_queue)
            book = data_fragment.get_book()
            if book is not None:
                self.books_side_table[book.get_title] = book
            self.mom.ack(tag)
            #if condition -> return

    def run(self):
        self.receive_all_books()
        while not self._exit:
            (data_fragment, tag) = self.mom.consume(self.reviews_queue)
            review = data_fragment.get_review()
            if review is not None:
                book = self.books_side_table[review.ger_book_title()]
                if book is not None:
                    data_fragment.set_book(book)
                    # updated_dict = update_data_fragment_step(DataFragment)
                    # for data in updated_dict:
                    #     self.mom.publish(updated_dict[data],data)
                    self.mom.publish(update_data_fragment_step(data_fragment))
                else: 
                    print("Error: Book not found")
            self.mom.ack(tag)

def main():
    joiner = Joiner()
    joiner.run()
   
if __name__ == "__main__":
    main()