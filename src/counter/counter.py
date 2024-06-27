import sys
import os
import signal
import socket
from multiprocessing import Process, Event
from log_manager.log_writer import LogWriter
from utils.structs.book import *
from utils.structs.review import *
import base64
from utils.structs.data_fragment import *
from utils.structs.data_chunk import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step
from dotenv import load_dotenv # type: ignore
import time
import numpy as np
import logging as logger
from log_manager.log_recoverer import LogRecoverer

load_dotenv()
NODE_TYPE=os.environ["NODE_TYPE"]
HARTBEAT_INTERVAL=int(os.environ["HARTBEAT_INTERVAL"])
MEDIC_IP_ADDRESSES=eval(os.environ.get("MEDIC_IPS"))
MEDIC_PORT=int(os.environ["MEDIC_PORT"])
CATEGORY_FILTER = "CATEGORY"
YEAR_FILTER = "YEAR"
TITLE_FILTER = "TITLE"
DISTINCT_FILTER = "COUNT_DISTINCT"
SENTIMENT_FILTER = "SENTIMENT"
MAX_AMOUNT_OF_FRAGMENTS = 800
TOP = "TOP"
MAX_QUERIES = 1

MAX_SLEEP = 10 # seconds
MULTIPLIER = 0.1

class Counter:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        log_recoverer = LogRecoverer(os.environ["LOG_PATH"])
        log_recoverer.recover_data()
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.info_queue = os.environ["INFO_QUEUE"]
        consumer_queues.append(self.info_queue)
        self.work_queue = consumer_queues[0]
        self.mom = MOM(consumer_queues)
        self.id= os.environ["ID"]
        self.event = None
        self.exit = False
        self.counted_data = log_recoverer.get_counted_data()
        self.books = log_recoverer.get_books()
        self.received_ids = log_recoverer.get_received_ids()
        self.ignore_ids = log_recoverer.get_ignore_ids()
        self.log_writer = LogWriter(os.environ["LOG_PATH"])
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)
    
    def sigterm_handler(self, signal, frame):
        self.exit = True
        if self.mom:
            self.mom.close()
        if self.log_writer:
            self.log_writer.close()
        if self.event:
            self.event.set()
    
    def clean_data_client(self, client_id):
        logger.info(f"Cleaning data from client {client_id}")
        if client_id in self.received_ids.keys():
            self.received_ids.pop(client_id)
        if client_id in self.counted_data.keys():
            self.counted_data.pop(client_id)
        self.ignore_ids.add(client_id)
        self.log_writer.log_ignore(client_id)
    
    def save_id(self, data_fragment: DataFragment) -> bool:
        client_id = data_fragment.get_client_id()
        query_id = data_fragment.get_query_id()
        id = data_fragment.get_id()
        if client_id in self.ignore_ids:
            return False
        self.received_ids[client_id] = self.received_ids.get(client_id, {})
        self.received_ids[client_id][query_id] = self.received_ids[client_id].get(query_id, set())
        if id in self.received_ids[client_id][query_id]:
            return False
        self.received_ids[client_id][query_id].add(id)
        return True
    
    def clean_data(self, query_id: str, client_id: str):
        if not client_id in self.counted_data.keys():
            return
        if query_id in self.counted_data[client_id].keys():
            self.counted_data[client_id].pop(query_id)
        if len(self.counted_data[client_id]) == 0:
            self.counted_data.pop(client_id)

    def count_data_fragment(self, data_fragment: DataFragment, event) -> List[DataFragment]:
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

        if query_info.filter_by_top():
            return self.count_top(data_fragment, query_id)

        bool_set = []
        for v in [group_by, count_distinct, average_column, percentile_data]:
            bool_set.append(v is not None)
        if [True, True, False, False] == bool_set: # for query 2
            return self.count_type_1(data_fragment, query_id, queries, group_data, value,event)
        if [True, True, True, False] == bool_set: # for queries 3 and 4
            return self.count_type_2(data_fragment, query_id, queries, group_data, value,event)
        if [True, False, False, True] == bool_set: # for query 5
            return self.count_type_3(data_fragment, query_id, queries, group_data, value, percentile_data[0],event)
        return []
    
    def count_top(self, data_fragment, query_id):
        results = []
        client_id = data_fragment.get_client_id()
        query_info = data_fragment.get_query_info()
        top_amount = query_info.get_top()[0]
        if not data_fragment.is_last(): 
            self.counted_data[client_id][query_id][TOP] = self.counted_data[client_id][query_id].get(TOP, [])
            added = False
            if len(self.counted_data[client_id][query_id][TOP]) < top_amount:
                self.counted_data[client_id][query_id][TOP].append(data_fragment)
                added = True
            else:
                lowest = self.counted_data[client_id][query_id][TOP][0]
                if data_fragment.get_query_info().get_average() > lowest.get_query_info().get_average():
                    self.counted_data[client_id][query_id][TOP][0] = data_fragment
                    added = True
            if added:
                self.counted_data[client_id][query_id][TOP] = sorted(self.counted_data[client_id][query_id][TOP], key=lambda fragment: fragment.get_query_info().get_average())
                df_str = base64.b64encode(data_fragment.to_bytes()).decode()
                count_info = {"TOP": df_str, "AMOUNT": top_amount}
                self.log_writer.log_counted_data(data_fragment, repr(count_info))
        else:
            top = self.counted_data[client_id][query_id][TOP]
            for fragment in top:
                if self.exit:
                    return results
                results.append(fragment)
            self.counted_data[client_id][query_id].pop(TOP)
            self.clean_data(query_id, client_id)
        return results

    def count_type_3(self, data_fragment, query_id, queries, group_data, value, percentile, event):
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
            self.books[group_data] = data_fragment.get_book() #TODO: redo this log if needed
            self.log_writer.log_book(data_fragment.get_book())
            count_info = {"PERCENTILE": percentile, "VALUE": value, "GROUP_DATA": group_data}
            self.log_writer.log_counted_data(data_fragment, repr(count_info))
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
                if event.is_set():
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

    def count_type_2(self, data_fragment, query_id, queries, group_data, value, event):
        results = []
        client_id = data_fragment.get_client_id()
        if not data_fragment.is_last():
            if group_data not in self.counted_data[client_id][query_id].keys():
                self.counted_data[client_id][query_id][group_data] = {"TOTAL": 0, "COUNT": 0}
            self.counted_data[client_id][query_id][group_data]["TOTAL"] += value
            self.counted_data[client_id][query_id][group_data]["COUNT"] += 1
            self.books[group_data] = data_fragment.get_book() #TODO: redo this log if needed
            self.log_writer.log_book(data_fragment.get_book())
            count_info = {"2": 2, "VALUE": value, "GROUP_DATA": group_data}
            self.log_writer.log_counted_data(data_fragment, repr(count_info))
        else:
            next_id = data_fragment.get_id() + 1
            for group_data in self.counted_data[client_id][query_id].keys():
                if event.is_set():
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

    def count_type_1(self, data_fragment, query_id, queries, group_data, value, event):
        results = []
        client_id = data_fragment.get_client_id()
        if not data_fragment.is_last():
            # group data is a list  
            if type(group_data) == list:
                for data in group_data:
                    if event.is_set():
                        return results
                    if data not in self.counted_data[client_id][query_id].keys():
                        self.counted_data[client_id][query_id][data] = set()
                    self.counted_data[client_id][query_id][data].add(value)
                    count_info = {"1": 1, "VALUE": value, "GROUP_DATA": data}
                    self.log_writer.log_counted_data(data_fragment, repr(count_info))
            else:
                logger.warning(f"Group data is not a list, it is a {type(group_data)}")
        else:
            next_id = data_fragment.get_id() + 1
            for key, value in self.counted_data[client_id][query_id].items():
                if event.is_set():
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
    

    def process_msg(self,event) -> bool:
        msg = self.mom.consume(self.work_queue)
        if not msg:
            return False
        data_chunk, tag = msg
        for data_fragment in data_chunk.get_fragments():
            if event.is_set():
                return
            if not self.save_id(data_fragment):
                continue
            results = self.count_data_fragment(data_fragment, event)

            if data_fragment.is_last():
                self.log_writer.log_query_ended(data_fragment)
                results.append(data_fragment)
                key = None
                fragments = []
                for results_data_fragment in results:
                    if event.is_set():
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
                self.log_writer.log_counted_data_sent(data_fragment)
                self.rewrite_logs(event)

                self.clean_data(data_fragment.get_query_id(), data_fragment.get_client_id())
        self.mom.ack(delivery_tag=tag)
        return True

    def inspect_info_queue(self, event) -> None:
        while not event.is_set():
            msg = self.mom.consume(self.info_queue)
            if not msg:
                return
            datafragment, tag = msg
            if datafragment.get_query_info().is_clean_flag():
                client_id = datafragment.get_client_id()
                logger.info(f"Received a clean flag for client {client_id}, cleaning data")
                self.clean_data_client(client_id)
                self.rewrite_logs(event)
            else:
                logger.error(f"Unexpected message in info queue: {datafragment}")
            self.mom.ack(tag)

    def run_counter(self, event):
        times_empty = 0
        while not event.is_set():
            try:
                self.inspect_info_queue(event)
                if not self.process_msg(event):
                    times_empty += 1
                    time.sleep(min(MAX_SLEEP, (times_empty**2) * MULTIPLIER))
                    continue
                times_empty = 0
            except Exception as e:
                logger.error(f"Error in counter: {e.with_traceback(None)}")
                event.set()

    def run(self):
        self.event = Event()
        counter_proccess = Process(target=self.run_counter, args=(self.event,))
        counter_proccess.start()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while not self.exit and counter_proccess.is_alive():
            msg = NODE_TYPE + "." + self.id + "$"
            try:
                for id,address in MEDIC_IP_ADDRESSES.items():
                    complete_addres = (address, MEDIC_PORT)
                    sock.sendto(msg.encode(), complete_addres)
                    logger.error(f"Hartbeat sent to medic with id: {id}")
            except Exception as e:
                logger.error(f"Error sending hartbeat: {e}")
            finally:
                time.sleep(HARTBEAT_INTERVAL)
        counter_proccess.join()

    def rewrite_logs(self, event):
        self.log_writer.close()
        log_rewriter = LogRecoverer(os.environ["LOG_PATH"])
        log_rewriter.rewrite_logs(event)
        log_rewriter.swap_files()
        self.log_writer.open()

def main():
    counter = Counter()
    counter.run()
    if not counter.exit:
        if counter.mom:
            counter.mom.close()
        if counter.log_writer:
            counter.log_writer.close()
   
if __name__ == "__main__":
    main()