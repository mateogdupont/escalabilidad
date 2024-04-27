import signal
from utils.mom import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.query_updater import *
from dotenv import load_dotenv
from multiprocessing import Process, Manager
import logging as logger
import signa
import sys
import os
import time
from textblob import TextBlob
from multiprocessing import Lock

def get_sentiment_score(text: str) -> float:
    return TextBlob(text).sentiment.polarity

def work(sleep_time, consumer_queues, work_queue, lock, ending_signal):
    mom = MOM(consumer_queues)
    while True:
        lock.acquire()
        if ending_signal.value:
            lock.release()
            break
        lock.release()
        data_fragment, delivery_tag = mom.consume(work_queue)
        if not data_fragment:
            logger.info(f"No data found in queue '{work_queue}', sleeping for {sleep_time} seconds.")
            time.sleep(sleep_time)
            continue
        logger.info("Received a data fragment.")
        review_text = data_fragment.get_review().get_text()
        sentiment_score = get_sentiment_score(review_text)
        logger.info(f"Sentiment score for review text '{review_text[:25]}...': {sentiment_score}")
        query_info = data_fragment.get_query_info().set_sentiment_score(sentiment_score)
        data_fragment.set_query_info(query_info)
        mom.ack(delivery_tag)
        mom.publish(update_data_fragment_step(data_fragment))

def update_signal(lock, ending_signal):
    lock.acquire()
    setattr(ending_signal, "value", True)
    lock.release()

def main() -> None:
    logger.basicConfig(stream=sys.stdout, level=logger.INFO)
    load_dotenv()

    repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
    sleep_time = int(os.environ["SLEEP_TIME"])

    consumer_queues = eval(repr_consumer_queues)
    work_queue = consumer_queues.keys()[0]

    n_threads = int(os.environ["N_THREADS"])

    lock = Lock()

    with Manager() as manager:
        ending_signal = manager.Bool(False)
        signal.signal(signal.SIGTERM, update_signal(lock, ending_signal))
        for i in range(n_threads):
            Process(target=work, args=(sleep_time, consumer_queues, work_queue, lock, ending_signal)).start()

if __name__ == "__main__":
    analysis_texts = ["I love this book!", "I hate this book!", "I don't know how I feel about this book."]
    querys = {5: 0}
    mom = MOM({})
    for i in range(len(analysis_texts)):
        review = Review(i, "Book", "user_id", "profile_name", "helpfulness", 5.0, 123456789, "Summary", analysis_texts[i])
        datafragment = DataFragment(querys, None, review)
        mom.publish({datafragment: "sentiment_analysis"})
    

    main()