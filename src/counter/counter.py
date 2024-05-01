import sys
import os
import signal
from multiprocessing import Process, Event
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.structs.data_chunk import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step
from dotenv import load_dotenv
import numpy as np
import logging as logger

CATEGORY_FILTER = "CATEGORY"
YEAR_FILTER = "YEAR"
TITLE_FILTER = "TITLE"
DISTINCT_FILTER = "COUNT_DISTINCT"
SENTIMENT_FILTER = "SENTIMENT"

class Counter:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
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
        book, review = data_fragment.get_book(), data_fragment.get_review()
        query_id, queries = data_fragment.get_query_id(), data_fragment.get_queries()

        group_data, value, percentile = self.get_counter_values(query_info, group_by, count_distinct, average_column, percentile_data, book, review)
        
        if query_id not in self.counted_data.keys():
            self.counted_data[query_id] = {}
        bool_set = []
        results = []
        for v in [group_by, count_distinct, average_column, percentile_data]:
            bool_set.append(v is not None)
        if [True, True, False, False] == bool_set: # for query 2
            self.count_type_1(data_fragment, query_id, queries, group_data, value, results)
        elif [True, True, True, False] == bool_set: # for queries 3 and 4
            self.count_type_2(data_fragment, query_info, query_id, group_data, value, results)
        elif [True, False, False, True] == bool_set: # for query 5
            self.count_type_3(data_fragment, query_info, query_id, group_data, value, percentile, results)
        return results

    def count_type_3(self, data_fragment, query_info, query_id, group_data, value, percentile, results):
        logger.info("Processing query 5")
        if group_data not in self.counted_data[query_id].keys():
            self.counted_data[query_id][group_data] = {"PERCENTILE": percentile, "VALUES": []}
        self.counted_data[query_id][group_data]["VALUES"].append(value)
        
        if not data_fragment.is_last():
            return
        
        percentile_90 = np.percentile(self.counted_data[query_id][group_data]["VALUES"], self.counted_data[query_id][group_data]["PERCENTILE"])
        query_info.set_percentile(percentile_90)
        data_fragment.set_query_info(query_info)
        results.append(data_fragment)

    def count_type_2(self, data_fragment, query_info, query_id, group_data, value, results):
        logger.info("Processing query 3 and 4")
        if group_data not in self.counted_data[query_id].keys():
            self.counted_data[query_id][group_data] = {"TOTAL": 0, "COUNT": 0}
        self.counted_data[query_id][group_data]["TOTAL"] += value
        self.counted_data[query_id][group_data]["COUNT"] += 1

        if not data_fragment.is_last():
            return
            
        query_info.set_n_distinct(self.counted_data[query_id][group_data]["COUNT"])
        query_info.set_average(self.counted_data[query_id][group_data]["TOTAL"] / self.counted_data[query_id][group_data]["COUNT"])
        data_fragment.set_query_info(query_info)
        results.append(data_fragment)

    def count_type_1(self, data_fragment, query_id, queries, group_data, value, results):
        # group data is a list
        if type(group_data) == list:
            for data in group_data:
                if data not in self.counted_data[query_id].keys():
                    self.counted_data[query_id][data] = set()
                self.counted_data[query_id][data].add(value)
        else:
            logger.warning(f"Group data is not a list, it is a {type(group_data)}")
        
        if not data_fragment.is_last():
            return

        base_data_fragment = DataFragment(queries.copy(), None, None)
        for key, value in self.counted_data[query_id].items():
            new_data_fragment = base_data_fragment.clone()
            new_query_info = QueryInfo()
            new_query_info.set_author(key)
            new_query_info.set_n_distinct(len(value))
            new_data_fragment.set_query_info(new_query_info)
            results.append(new_data_fragment)
            if len(value) >= 10:
                logger.info(f"Author: {key} has {len(value)} decades")
            if key == AUTHOR:
                logger.info(f"Author {AUTHOR} has {len(value)} decades")

    def get_counter_values(self, query_info, group_by, count_distinct, average_column, percentile_data, book, review):
        group_data, value, percentile = None, None, None

        if (group_by == "AUTHOR") and (book is not None):
            group_data = book.get_authors()
        elif (group_by == "BOOK_TITLE") and (review is not None):
            group_data = review.get_book_title()
        
        if (count_distinct == "DECADE") and (book is not None):
            if AUTHOR in book.get_authors():
                logger.info(f"Author {AUTHOR} found | year: {book.get_published_year()}")
                logger.info(f"Author {AUTHOR} found | title: {book.get_title()}")
            value = (book.get_published_year() // 10) * 10
        elif (average_column == "SCORE") and (review is not None):
            value = review.get_score()
        elif (percentile_data is not None) and (query_info is not None):
            percentile = query_info.get_percentile()[0]
            value = query_info.get_sentiment()
        return group_data, value, percentile
            
    def run(self):
        while not self._exit:
            msg = self.mom.consume(self.work_queue)
            if not msg:
                continue # TODO: change this
            data_chunk, tag = msg
            for data_fragment in data_chunk.get_fragments():
                results = self.count_data_fragment(data_fragment)

                if len(results) > 0:
                    results[-1].set_as_last()

                key = None
                fragments = []
                for results_data_fragment in results:
                    steps = update_data_fragment_step(results_data_fragment)
                    # for data, key in steps.items():
                    #     self.mom.publish(DataChunk([data]), key)
                    fragments.extend(steps.keys())
                    key = steps[results_data_fragment]
                self.mom.publish(DataChunk(fragments), key)
            self.mom.ack(tag)

def main():
    filter = Counter()
    filter.run()
   
if __name__ == "__main__":
    main()