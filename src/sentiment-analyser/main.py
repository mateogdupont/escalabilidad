from utils.mom.mom import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from dotenv import load_dotenv
import logging as logger
import sys
import os
import time
from textblob import TextBlob

def get_sentiment_score(text: str) -> float:
    return TextBlob(text).sentiment.polarity

def main() -> None:
    logger.basicConfig(stream=sys.stdout, level=logger.INFO)
    load_dotenv()

    repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
    sleep_time = int(os.environ["SLEEP_TIME"])

    consumer_queues = eval(repr_consumer_queues)
    work_queue = consumer_queues.keys()[0]
    logger.info(f"consumer_queues: {consumer_queues}")

    mom = MOM(consumer_queues)

    while True:
        data_fragment, delivery_tag = mom.consume(work_queue)
        if not data_fragment:
            logger.info(f"No data found in queue '{work_queue}', sleeping for {sleep_time} seconds.")
            time.sleep(sleep_time)
            continue
        logger.info("Received a data fragment.")
        review_text = data_fragment.get_review().get_text()
        sentiment_score = get_sentiment_score(review_text)
        logger.info(f"Sentiment score for review text '{review_text[:25]}...': {sentiment_score}")
        data_fragment.get_query_info().set_sentiment_score(sentiment_score)
        # TODO: continue




    
main()