import sys
import os
import signal
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step

class Joiner:
    def __init__(self):
        self._exit = False
        self.books_side_table = {}
        self.mom = MOM({'filter':None,'joiner_books':None,'joiner_reviews':None,'results':None})
        signal.signal(signal.SIGTERM, self.sigterm_handler)
    
    def sigterm_handler(self):
        self._exit = True

    #TODO: Add stop condition when finish reading books
    def receive_all_books(self):
        while not self._exit:
            (data_fragment, tag) = self.mom.consume("joiner_books")
            book = data_fragment.get_book()
            if book is not None:
                self.books_side_table[book.get_title] = book
            self.mom.ack(tag)
            #if condition -> return

    def run(self):
        self.receive_all_books()
        while not self._exit:
            (data_fragment, tag) = self.mom.consume("joiner_reviews")
            review = data_fragment.get_review()
            if review is not None:
                book = self.books_side_table[review.ger_book_title()]
                if book is not None:
                    data_fragment.set_book(book)
                    updated_dict = update_data_fragment_step(DataFragment)
                    for data in updated_dict:
                        self.mom.publish(updated_dict[data],data)
                else: 
                    print("Error: Book not found")
            self.mom.ack(tag)

def main():
    joiner = Joiner()
    joiner.run()
   
if __name__ == "__main__":
    main()