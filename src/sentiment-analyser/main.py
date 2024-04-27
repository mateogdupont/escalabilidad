import signal
from utils.mom.mom import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.query_updater import *
from dotenv import load_dotenv
from multiprocessing import Process, Manager
import logging as logger
import sys
import os
import time
from textblob import TextBlob
from multiprocessing import Lock

def get_sentiment_score(text: str) -> float:
    return TextBlob(text).sentiment.polarity

def work(sleep_time, consumer_queues, work_queue, lock, ending_signal):
    mom = MOM(consumer_queues)
    # mom = MOM({"books-analyser.data_processor.sentiment_analysis": {'x-max-priority': 5}})
    while True:
        # lock.acquire()
        # if ending_signal.value:
        #     lock.release()
        #     break
        # lock.release()
        msg = mom.consume(work_queue)
        if not msg:
            logger.info(f"No data found in queue, sleeping for {sleep_time} seconds.")
            time.sleep(sleep_time)
            continue
        data_fragment, delivery_tag = msg
        logger.info("Received a data fragment.")
        review_text = data_fragment.get_review().get_text()
        sentiment_score = get_sentiment_score(review_text)
        logger.info(f"Sentiment score for review text '{review_text[:30]}...': {sentiment_score}")
        query_info = data_fragment.get_query_info().set_sentiment(sentiment_score)
        data_fragment.set_query_info(query_info)
        mom.ack(delivery_tag)
        if len(update_data_fragment_step(data_fragment).keys()) > 1:
            logger.warning("More than one data fragment returned from update_data_fragment_step")
            logger.info(f"original data fragment with querys {data_fragment.get_querys()}")
            for data_fragment, _ in update_data_fragment_step(data_fragment).items():
                logger.info(f"Publishing data fragment with querys {data_fragment.get_querys()}")
            return
        mom.publish(update_data_fragment_step(data_fragment))
        # for data_fragment, routing_key in update_data_fragment_step(data_fragment).items():
        #     mom.publish(routing_key, data_fragment)

# def update_signal(lock, ending_signal):
#     lock.acquire()
#     setattr(ending_signal, "value", True)
#     lock.release()

def main() -> None:
    logger.basicConfig(stream=sys.stdout, level=logger.INFO)
    load_dotenv()

    repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
    sleep_time = int(os.environ["SLEEP_TIME"])

    consumer_queues = eval(repr_consumer_queues)
    work_queue = list(consumer_queues.keys())[0]

    n_threads = int(os.environ["N_THREADS"])

    lock = Lock()

    with Manager() as manager:
        ending_signal = False
        # ending_signal = manager.Bool(False)
        # signal.signal(signal.SIGTERM, update_signal(lock, ending_signal))
        for i in range(n_threads):
            Process(target=work, args=(sleep_time, consumer_queues, work_queue, lock, ending_signal)).start()

if __name__ == "__main__":
    main()