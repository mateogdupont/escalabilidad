import signal
import os
from utils.structs.data_chunk import *
from utils.mom.mom import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.query_updater import *
from log_writer import *
from dotenv import load_dotenv # type: ignore
import logging as logger
import sys
import os
import time
from textblob import TextBlob # type: ignore

def get_sentiment_score(text: str) -> float:
    return round(TextBlob(text).sentiment.polarity, 5)

CATEGORY_FILTER = "CATEGORY"
YEAR_FILTER = "YEAR"
TITLE_FILTER = "TITLE"
DISTINCT_FILTER = "COUNT_DISTINCT"
SENTIMENT_FILTER = "SENTIMENT"
MAX_AMOUNT_OF_FRAGMENTS = 800
TIMEOUT = 50

class Analyzer:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        load_dotenv()
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.work_queue = list(consumer_queues.keys())[0]
        self.mom = MOM(consumer_queues)
        self.results = {}
        self.received_ids = {}
        self.exit = False
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)
        log_queue = os.environ["LOG_QUEUE"]
        log_key = os.environ["LOG_KEY"]
        self.log_writer = LogWriter(log_queue, log_key)
    
    def sigterm_handler(self, signal,frame):
        self.exit = True
    
    def save_id(self, data_fragment: DataFragment) -> bool:
        client_id = data_fragment.get_client_id() # TODO: review with feat-multiclient branch
        query_id = data_fragment.get_query_id()
        id = data_fragment.get_id()
        self.received_ids[client_id] = self.received_ids.get(client_id, {})
        self.received_ids[client_id][query_id] = self.received_ids[client_id].get(query_id, set())
        if id in self.received_ids[client_id][query_id]:
            logger.warning("-----------------------------------------------")
            logger.warning(f"Repeated id: {id} from client: {client_id} query: {query_id}")
            logger.warning(f"Data saved: {self.received_ids[client_id][query_id][id]}")
            logger.warning(f"Data received: {data_fragment.to_human_readable()}")
            logger.warning("-----------------------------------------------")
            return False
        self.received_ids[client_id][query_id].add(id)
        return True

    def add_and_try_to_send_chunk(self, fragment: DataFragment, node: str):
        if self.exit:
            return
        if not node in self.results.keys():
            self.results[node] = []
        self.results[node].append(fragment)
        if len(self.results[node]) == MAX_AMOUNT_OF_FRAGMENTS or fragment.is_last():
            data_chunk = DataChunk(self.results[node])
            self.mom.publish(data_chunk, node)
            self.log_writer.log_result_sent(node)
            self.results[node] = []
    
    def run(self):
        while not self.exit:
            msg = self.mom.consume(self.work_queue)
            if not msg:
                continue
            data_chunk, tag = msg
            
            for data_fragment in data_chunk.get_fragments():
                if not self.save_id(data_fragment):
                    continue
                if self.exit:
                    return
                
                if not data_fragment.is_last():
                    self.analyze(data_fragment)
                else:
                    pass
                    self.log_writer.log_query_ended(data_fragment)

                # the text is no longer needed
                review = data_fragment.get_review()
                review.set_text(None)
                data_fragment.set_review(review)

                next_steps = update_data_fragment_step(data_fragment)
                
                if not data_fragment.is_last():
                    list_next_steps = [(fragment, key) for fragment, key in next_steps.items()]
                    self.log_writer.log_result(list_next_steps)
                
                for fragment, key in next_steps.items():
                    self.add_and_try_to_send_chunk(fragment, key)
            self.mom.ack(tag)

    def analyze(self, data_fragment):
        review_text = data_fragment.get_review().get_text()
        sentiment_score = get_sentiment_score(review_text)
        query_info = data_fragment.get_query_info()
        query_info.set_sentiment(sentiment_score)
        data_fragment.set_query_info(query_info)

def main() -> None:
    analyzer = Analyzer()
    analyzer.run()
    if not analyzer.exit:
        analyzer.mom.close()

if __name__ == "__main__":
    main()