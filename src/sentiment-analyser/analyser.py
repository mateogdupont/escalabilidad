import signal
import os
from utils.structs.data_chunk import *
from utils.mom.mom import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.query_updater import *
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
MAX_AMOUNT_OF_FRAGMENTS = 500
TIMEOUT = 50

class Analyser:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        load_dotenv()
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.work_queue = list(consumer_queues.keys())[0]
        self.mom = MOM(consumer_queues)
        self.results = {}
        self.exit = False
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)
    
    def sigterm_handler(self, signal,frame):
        self.exit = True

    def add_and_try_to_send_chunk(self, fragment: DataFragment, node: str):
        if self.exit:
            return
        if not node in self.results.keys():
            self.results[node] = []
        self.results[node].append(fragment)
        if len(self.results[node]) == MAX_AMOUNT_OF_FRAGMENTS or fragment.is_last():
            data_chunk = DataChunk(self.results[node])
            self.mom.publish(data_chunk, node)
            self.results[node] = []
    
    def run(self):
        while not self.exit:
            msg = self.mom.consume(self.work_queue)
            if not msg:
                continue
            data_chunk, tag = msg
            
            for data_fragment in data_chunk.get_fragments():
                if (not data_fragment.is_last()) and (not self.exit):
                    review_text = data_fragment.get_review().get_text()
                    sentiment_score = get_sentiment_score(review_text)
                    query_info = data_fragment.get_query_info()
                    query_info.set_sentiment(sentiment_score)
                    data_fragment.set_query_info(query_info)
                for fragment, key in update_data_fragment_step(data_fragment).items():
                    self.add_and_try_to_send_chunk(fragment, key)
            
            self.mom.ack(tag)

def main() -> None:
    analyser = Analyser()
    analyser.run()
    if not analyser.exit:
        analyser.mom.close()

if __name__ == "__main__":
    main()