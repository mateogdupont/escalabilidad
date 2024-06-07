import sys
import os
import signal
from multiprocessing import Process, Event
from counter.log_writer import LogWriter
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.structs.data_chunk import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step
from dotenv import load_dotenv # type: ignore
import time
import numpy as np
import logging as logger

CATEGORY_FILTER = "CATEGORY"
YEAR_FILTER = "YEAR"
TITLE_FILTER = "TITLE"
DISTINCT_FILTER = "COUNT_DISTINCT"
SENTIMENT_FILTER = "SENTIMENT"
MAX_AMOUNT_OF_FRAGMENTS = 800

PATH = "counter.log"

class Counter:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        load_dotenv()
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.work_queue = list(consumer_queues.keys())[0]
        self.mom = MOM(consumer_queues)
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)
        self.exit = False
        self.counted_data = {}
        self.books = {}
        self.received_ids = {}
        self.log_writer = LogWriter(PATH)
    
    def sigterm_handler(self, signal, frame):
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
        if not client_id in self.counted_data.keys():
            return
        if query_id in self.counted_data[client_id].keys():
            self.counted_data[client_id].pop(query_id)
        if len(self.counted_data[client_id]) == 0:
            self.counted_data.pop(client_id)

    def count_data_fragment(self, data_fragment: DataFragment) -> List[DataFragment]:
        query_info = data_fragment.get_query_info()
        group_by, count_distinct, average_column, percentile_data = query_info.get_counter_params()
        book, review = data_fragment.get_book(), data_fragment.get_review()
        query_id, queries = data_fragment.get_query_id(), data_fragment.get_queries()
        client_id = data_fragment.get_client_id()

        group_data, value = self.get_counter_values(query_info, group_by, count_distinct, average_column, percentile_data, book, review)
        
        if not client_id in self.counted_data.keys():
            self.counted_data[client_id] = {}
        if query_id not in self.counted_data[client_id].keys():
            self.counted_data[client_id][query_id] = {}
        bool_set = []
        for v in [group_by, count_distinct, average_column, percentile_data]:
            bool_set.append(v is not None)
        if [True, True, False, False] == bool_set: # for query 2
            return self.count_type_1(data_fragment, query_id, queries, group_data, value)
        if [True, True, True, False] == bool_set: # for queries 3 and 4
            return self.count_type_2(data_fragment, query_id, queries, group_data, value)
        if [True, False, False, True] == bool_set: # for query 5
            return self.count_type_3(data_fragment, query_id, queries, group_data, value, percentile_data[0])
        return []

    def count_type_3(self, data_fragment, query_id, queries, group_data, value, percentile):
        results = []
        client_id = data_fragment.get_client_id()
        if not data_fragment.is_last():    
            if value is None:
                logger.error(f"Value is None | {data_fragment.to_json()}")
                return results
            if percentile is None:
                logger.error(f"Percentile is None")
                return results
            if group_data not in self.counted_data[client_id][query_id].keys():
                self.counted_data[client_id][query_id][group_data] = {"PERCENTILE": percentile, "VALUES": []}
            self.counted_data[client_id][query_id][group_data]["VALUES"].append(value)
            self.books[group_data] = data_fragment.get_book()
        else:
            sentiment_scores = {}
            percentile_number = None
            next_id = data_fragment.get_id() + 1
            for group_data in self.counted_data[client_id][query_id].keys():
                if len(self.counted_data[client_id][query_id][group_data]["VALUES"]) == 0 or not self.counted_data[client_id][query_id][group_data]["PERCENTILE"]:
                    logger.error(f"Error calculating percentile. Discarding group data: {group_data}")
                    continue
                percentile_number = self.counted_data[client_id][query_id][group_data]["PERCENTILE"]
                average_score = float(np.mean(self.counted_data[client_id][query_id][group_data]["VALUES"]))
                sentiment_scores[group_data] = average_score
            if not percentile_number:
                logger.error(f"Percentile is None")
                return results
            percentile_result = float(np.percentile(list(sentiment_scores.values()), percentile_number))
            for group_data in self.counted_data[client_id][query_id].keys():
                if self.exit:
                    return results
                new_data_fragment = DataFragment(next_id, queries.copy(), None, None, client_id)
                next_id += 1
                new_query_info = QueryInfo()
                new_query_info.set_percentile(percentile_result)
                new_query_info.set_sentiment(sentiment_scores[group_data])
                new_data_fragment.set_query_info(new_query_info)
                review = Review.with_minimum_data(title=group_data, score=0.0)
                new_data_fragment.set_review(review)
                new_data_fragment.set_book(self.books[group_data])
                results.append(new_data_fragment)
        return results

    def count_type_2(self, data_fragment, query_id, queries, group_data, value):
        results = []
        client_id = data_fragment.get_client_id()
        if not data_fragment.is_last():
            if group_data not in self.counted_data[client_id][query_id].keys():
                self.counted_data[client_id][query_id][group_data] = {"TOTAL": 0, "COUNT": 0}
            self.counted_data[client_id][query_id][group_data]["TOTAL"] += value
            self.counted_data[client_id][query_id][group_data]["COUNT"] += 1
            self.books[group_data] = data_fragment.get_book()
        else:
            next_id = data_fragment.get_id() + 1
            for group_data in self.counted_data[client_id][query_id].keys():
                if self.exit:
                    return results
                new_data_fragment = DataFragment(next_id, queries.copy(), None, None, client_id)
                next_id += 1
                new_query_info = QueryInfo()
                new_query_info.set_n_distinct(self.counted_data[client_id][query_id][group_data]["COUNT"])
                new_query_info.set_average(self.counted_data[client_id][query_id][group_data]["TOTAL"] / self.counted_data[client_id][query_id][group_data]["COUNT"])
                new_data_fragment.set_query_info(new_query_info)
                review = Review.with_minimum_data(title=group_data,score=0.0)
                new_data_fragment.set_review(review)
                new_data_fragment.set_book(self.books[group_data])
                results.append(new_data_fragment)
        return results

    def count_type_1(self, data_fragment, query_id, queries, group_data, value):
        results = []
        client_id = data_fragment.get_client_id()
        if not data_fragment.is_last():
            # group data is a list  
            if type(group_data) == list:
                for data in group_data:
                    if self.exit:
                        return results
                    if data not in self.counted_data[client_id][query_id].keys():
                        self.counted_data[client_id][query_id][data] = set()
                    self.counted_data[client_id][query_id][data].add(value)
            else:
                logger.warning(f"Group data is not a list, it is a {type(group_data)}")
        else:
            next_id = data_fragment.get_id() + 1
            for key, value in self.counted_data[client_id][query_id].items():
                if self.exit:
                    return results
                new_data_fragment = DataFragment(next_id, queries.copy(), None, None, client_id)
                next_id += 1
                new_query_info = QueryInfo()
                new_query_info.set_author(key)
                new_query_info.set_n_distinct(len(value))
                new_data_fragment.set_query_info(new_query_info)
                results.append(new_data_fragment)
        return results

    def get_counter_values(self, query_info, group_by, count_distinct, average_column, percentile_data, book, review):
        group_data, value, percentile = None, None, None

        if (group_by == "AUTHOR") and (book is not None):
            group_data = book.get_authors()
        elif (group_by == "BOOK_TITLE") and (review is not None):
            group_data = review.get_book_title()
        if (count_distinct == "DECADE") and (book is not None):
            value = (book.get_published_year() // 10) * 10
        elif (average_column == "SCORE") and (review is not None):
            value = review.get_score()
        elif (percentile_data is not None) and (query_info.get_sentiment() is not None):
            value = query_info.get_sentiment()
        return group_data, value
            
    def run(self):
        while not self.exit:
            msg = self.mom.consume(self.work_queue)
            if not msg:
                continue
            data_chunk, tag = msg
            for data_fragment in data_chunk.get_fragments():
                if self.exit:
                    return
                if not self.save_id(data_fragment):
                    continue
                results = self.count_data_fragment(data_fragment)

                if data_fragment.is_last():
                    self.log_writer.log_query_ended(data_fragment)
                    self.send_results(data_fragment, results)
                    self.log_writer.log_counted_data_sent(data_fragment)
                    self.clean_data(data_fragment.get_query_id(), data_fragment.get_client_id())
                else:
                    client_id = data_fragment.get_client_id()
                    query_id = data_fragment.get_query_id()
                    counted_data = self.counted_data(client_id, query_id)
                    self.log_writer.log_counted_data(data_fragment, counted_data)
            self.mom.ack(tag)

    def send_results(self, data_fragment, results):
        results.append(data_fragment)
        key = None
        fragments = []
        for results_data_fragment in results:
            if self.exit:
                return
            steps = update_data_fragment_step(results_data_fragment)
            fragments.extend(steps.keys())
            key = list(steps.values())[0]
            chunk = DataChunk(fragments)
            if len(fragments) >= MAX_AMOUNT_OF_FRAGMENTS or chunk.contains_last_fragment():
                self.mom.publish(chunk, key)
                fragments = []
        if len(fragments) > 0:
            self.mom.publish(DataChunk(fragments), key)

def main():
    counter = Counter()
    counter.run()
    if not counter.exit:
        counter.mom.close()
   
if __name__ == "__main__":
    main()