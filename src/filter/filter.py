import sys
import os
import signal
from multiprocessing import Process, Event
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step

CATEGORY_FILTER = "CATEGORY"
YEAR_FILTER = "YEAR"
TITLE_FILTER = "TITLE"
DISTINCT_FILTER = "COUNT_DISTINCT"
SENTIMENT_FILTER = "SENTIMENT"

already_performed = set()

class Filter:
    def __init__(self):
        self._exit = False
        self.mom = MOM({"books-analyser.data_processor.filter": {'x-max-priority': 5}})
        signal.signal(signal.SIGTERM, self.sigterm_handler)
    
    def sigterm_handler(self):
        self._exit = True

    def filter_data_fragment(self, data_fragment: DataFragment) -> bool:
        query_info = data_fragment.get_query_info()
        filter_on, word, min_value, max_value = query_info.get_filter_params()
        book = data_fragment.get_book()
        query_info = data_fragment.get_query_info()

        if (filter_on == CATEGORY_FILTER) and (book is not None):
            already_performed.add(CATEGORY_FILTER)
            return word in book.get_categories()
        elif (filter_on == YEAR_FILTER) and (book is not None):
            already_performed.add(YEAR_FILTER)
            book_year = book.get_published_date().year
            return  min_value < book_year < max_value
        elif (filter_on == TITLE_FILTER) and (book is not None):
            already_performed.add(TITLE_FILTER)
            return word in book.get_title()
        elif (filter_on == DISTINCT_FILTER) and (query_info is not None):
            already_performed.add(DISTINCT_FILTER)
            return query_info.get_n_distinct() >= min_value
        elif (filter_on == SENTIMENT_FILTER) and (query_info is not None):
            already_performed.add(SENTIMENT_FILTER)
            return query_info.get_sentimentt() >= min_value
        else:
            print("Error processing data in filter")
            print(f"Filter on: {filter_on}")
            print(f"Word: {word}")
            print(f"Min value: {min_value}")
            print(f"Max value: {max_value}")
            print(data_fragment.get_querys())
            print(data_fragment.get_book())
            print(data_fragment.get_review())
            print(data_fragment.get_query_info())
            print(data_fragment.to_json())
        return False

    def run(self):
        while not self._exit:
            msg = self.mom.consume("books-analyser.data_processor.filter")
            if not msg:
                print([a for a in already_performed])
                return # TODO: change this
            data_fragment, tag = msg
            if self.filter_data_fragment(data_fragment):
                updated_dict = update_data_fragment_step(data_fragment)
                for data, key in updated_dict.items():
                    self.mom.publish(key, data)
            self.mom.ack(tag)

def main():
    filter = Filter()
    filter.run()
   
if __name__ == "__main__":
    main()