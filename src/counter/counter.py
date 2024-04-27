import sys
import os
import signal
from multiprocessing import Process, Event
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

class Counter:
    def __init__(self):
        load_dotenv()
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.work_queue = list(consumer_queues.keys())[0]
        self.mom = MOM(consumer_queues)
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        self._exit = False
        self.counted_data = {}
    
    def sigterm_handler(self):
        self._exit = True

    def count_data_fragment(self, data_fragment: DataFragment) -> bool:
        query_info = data_fragment.get_query_info()
        group_by, count_distinct, average_column, percentile_data = query_info.get_counter_params()
        book = data_fragment.get_book()
        review = data_fragment.get_review()
        query_info = data_fragment.get_query_info()
        query_id = data_fragment.get_query_id()

        group_data = None
        value = None
        percentile = None


        if (group_by == "AUTHOR") and (book is not None):
            group_data = book.get_authors()
        elif (group_by == "BOOK_TITLE") and (review is not None):
            group_data = review.get_book_title()
        
        if (count_distinct == "DECADE") and (book is not None):
            value = book.get_published_date().year // 10 * 10
        elif (average_column == "SCORE") and (review is not None):
            value = review.get_score()
        elif (percentile_data is not None) and (query_info is not None):
            percentile = query_info.get_percentile()[0]
            value = query_info.get_sentiment()
        
        if query_id not in self.counted_data.keys():
            self.counted_data[query_id] = {}
        bool_set = []
        results_data_fragment = None
        for value in [group_by, count_distinct, average_column, percentile_data]:
            bool_set.append(value is not None)
        if [True, True, False, False] == bool_set:
            # if group data is a list
            if type(group_data) == list:
                for data in group_data:
                    if data not in self.counted_data[query_id].keys():
                        self.counted_data[query_id][data] = set()
                    self.counted_data[query_id][data].add(value)
            # TODO: create the datafragment with results
        elif [True, True, True, False] == bool_set:
            if group_data not in self.counted_data[query_id].keys():
                self.counted_data[query_id][group_data] = {"TOTAL": 0, "COUNT": 0}
            self.counted_data[query_id][group_data]["TOTAL"] += value
            self.counted_data[query_id][group_data]["COUNT"] += 1
            # TODO: create the datafragment with results
        elif [True, False, False, True] == bool_set:
            if group_data not in self.counted_data[query_id].keys():
                self.counted_data[query_id][group_data] = {"PERCENTILE": percentile, "VALUES": []}
            self.counted_data[query_id][group_data]["VALUES"].append(value)
            # TODO: create the datafragment with results
        
        return results_data_fragment
            

        

    def run(self):
        while not self._exit:
            msg = self.mom.consume(self.work_queue)
            if not msg:
                return # TODO: change this
            data_fragment, tag = msg
            results_data_fragment = self.count_data_fragment(data_fragment)
            
            if data_fragment.is_last():
                self.mom.publish(update_data_fragment_step(results_data_fragment))

            self.mom.ack(tag)

def main():
    filter = Counter()
    filter.run()
   
if __name__ == "__main__":
    main()