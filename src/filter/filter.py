import signal
import os
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step
from dotenv import load_dotenv

CATEGORY_FILTER = "CATEGORY"
YEAR_FILTER = "YEAR"
TITLE_FILTER = "TITLE"
DISTINCT_FILTER = "COUNT_DISTINCT"
SENTIMENT_FILTER = "SENTIMENT"

class Filter:
    def __init__(self):
        load_dotenv()
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.work_queue = list(consumer_queues.keys())[0]
        self.mom = MOM(consumer_queues)
        self.top_ten = []
        # self.mom = MOM({"books-analyser.data_processor.filter": {'x-max-priority': 5}})
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        self._exit = False
    
    def sigterm_handler(self):
        self._exit = True

    def filter_data_fragment(self, data_fragment: DataFragment) -> bool:
        query_info = data_fragment.get_query_info()
        filter_on, word, min_value, max_value = query_info.get_filter_params()
        book = data_fragment.get_book()
        query_info = data_fragment.get_query_info()

        if (filter_on == CATEGORY_FILTER) and (book is not None):
            return word in book.get_categories()
        elif (filter_on == YEAR_FILTER) and (book is not None):
            book_year = book.get_published_year()
            return  min_value < book_year < max_value
        elif (filter_on == TITLE_FILTER) and (book is not None):
            return word in book.get_title()
        elif (filter_on == DISTINCT_FILTER) and (query_info is not None):
            return query_info.get_n_distinct() >= min_value
        elif (filter_on == SENTIMENT_FILTER) and (query_info is not None):
            return query_info.get_sentimentt() >= min_value
        else:
            #TODO: Apply filter of top10
            pass
            if data_fragment.is_last():
                #Send all data
                self.top_ten = []
                return False
            #Check if is a top 10 and insert it in order
 
        return False

    def run(self):
        while not self._exit:
            msg = self.mom.consume(self.work_queue)
            if not msg:
                return # TODO: change this
            data_fragment, tag = msg
            if self.filter_data_fragment(data_fragment):
                # updated_dict = update_data_fragment_step(data_fragment)
                # for data, key in updated_dict.items():
                #     self.mom.publish(key, data)
                self.mom.publish(update_data_fragment_step(data_fragment))
            self.mom.ack(tag)

def main():
    filter = Filter()
    filter.run()
   
if __name__ == "__main__":
    main()