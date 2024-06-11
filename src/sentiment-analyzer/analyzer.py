import signal
import os
import socket
from utils.structs.data_chunk import *
from utils.mom.mom import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.query_updater import *
from multiprocessing import Process, Event
from dotenv import load_dotenv # type: ignore
import logging as logger
import sys
import os
import time
from textblob import TextBlob # type: ignore

def get_sentiment_score(text: str) -> float:
    return round(TextBlob(text).sentiment.polarity, 5)

load_dotenv()
NODE_TYPE=os.environ["NODE_TYPE"]
HARTBEAT_INTERVAL=int(os.environ["HARTBEAT_INTERVAL"])
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
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.work_queue = list(consumer_queues.keys())[0]
        self.mom = MOM(consumer_queues)
        self.medic_addres = (os.environ["MEDIC_IP"], int(os.environ["MEDIC_PORT"]))
        self.id= os.environ["ID"]
        self.event = None
        self.results = {}
        self.received_ids = {}
        self.exit = False
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)
    
    def sigterm_handler(self, signal,frame):
        self.exit = True
        self.mom.close()
        if self.event:
            self.event.set()
    
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

    def add_and_try_to_send_chunk(self, fragment: DataFragment, node: str, event):
        if event.is_set():
            return
        if not node in self.results.keys():
            self.results[node] = []
        self.results[node].append(fragment)
        if len(self.results[node]) == MAX_AMOUNT_OF_FRAGMENTS or fragment.is_last():
            data_chunk = DataChunk(self.results[node])
            self.mom.publish(data_chunk, node)
            self.results[node] = []

    def run_analizer(self, event):
        while not event.is_set():
            msg = self.mom.consume(self.work_queue)
            if not msg:
                continue
            data_chunk, tag = msg
            
            for data_fragment in data_chunk.get_fragments():
                if not self.save_id(data_fragment):
                    continue
                if (not data_fragment.is_last()) and (not event.is_set()):
                    review_text = data_fragment.get_review().get_text()
                    sentiment_score = get_sentiment_score(review_text)
                    query_info = data_fragment.get_query_info()
                    query_info.set_sentiment(sentiment_score)
                    data_fragment.set_query_info(query_info)
                for fragment, key in update_data_fragment_step(data_fragment).items():
                    self.add_and_try_to_send_chunk(fragment, key, event)
            
            self.mom.ack(tag)

    def run(self):
        self.event = Event()
        analyzer_proccess = Process(target=self.run_analizer, args=(self.event,))
        analyzer_proccess.start()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while not self.exit and analyzer_proccess.is_alive():
            msg = NODE_TYPE + "." + self.id + "$"
            logger.info(f"Voy a mandar {msg} a { self.medic_addres}")
            sock.sendto(msg.encode(), self.medic_addres)
            time.sleep(HARTBEAT_INTERVAL)
        analyzer_proccess.join()

def main() -> None:
    analyzer = Analyzer()
    analyzer.run()
    if not analyzer.exit:
        analyzer.mom.close()

if __name__ == "__main__":
    main()