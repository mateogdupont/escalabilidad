import csv
import signal
import os
import re
from typing import List
from multiprocessing import Process, Event
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.structs.data_chunk import *
from utils.stream_communications import *
from dotenv import load_dotenv # type: ignore
import logging as logger
import sys

year_regex = re.compile('[^\d]*(\d{4})[^\d]*')

CHUNK_SIZE = 500
BOOKS_FILE_NAME = "books_data.csv"
REVIEWS_FILE_NAME = "Books_rating.csv"
RESULTS_FILE_NAME = "Results.csv"
RESULTS_COLUMNS = ['Query','Title','Author','Publisher','Publised Year','Categories','Distinc Amount', 'Average', 'Sentiment', 'Percentile']
BOOKS_RELEVANT_COLUMNS = [0,1,2,5,6,8,9]
REVIEWS_RELEVANT_COLUMNS = [0,1,5,6,8,9]
BOOKS_ARGUMENT_AMOUNT = 7
REVIEW_ARGUMENT_AMOUNT = 6

# Book db:
# Title|description|authors|image|previewLink|publisher|pubishedDate|infoLink|categories|ratingCount

# Reviews db:
# Id|Title|Price|User_id|profileName|review/helpfulness|review/score|review/time|review/summary|review/text

class Client:
    def __init__(self, data_path: str, queries: 'dict[int, int]', socket):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        load_dotenv()
        self._data_path = data_path
        self._queries = queries
        self._stop = False
        self._event = None
        self.total = 0
        self.socket = socket
        self.data = DataChunk([])
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def sigterm_handler(self, signal,frame):
        self._stop = True
        if self._event:
            self._event.set()

    def read_chunk_with_columns(self,reader, columns: List[int]):
        chunk = []
        for row in reader:
            element = [row[i] for i in columns]
            chunk.append(element)
            if len(chunk) == CHUNK_SIZE or self._stop:
                return chunk
        return chunk

    def parse_year(self,read_date: str):
        if read_date:
            result = year_regex.search(read_date)
            return result.group(1) if result else None
        return None

    def parse_data(self, data):
        book = None
        review = None
        if len(data) == BOOKS_ARGUMENT_AMOUNT:
            publish_year = self.parse_year(data[4])
            book = Book(data[0],data[1],data[2],None,None,data[3],publish_year,None,data[5],data[6])
        elif len(data) == REVIEW_ARGUMENT_AMOUNT:
            review = Review(data[0],data[1],None,None,data[2],data[3],None,data[4],data[5])
        else:
            return None
        queries_copy = self._queries.copy()
        return DataFragment(queries_copy, book , review)
    
    def _send_data_chunk(self,data_chunk):
        for data in data_chunk:
            parsed_data = self.parse_data(data)
            if parsed_data != None:
                self.data.add_fragment(parsed_data)
                if self.data.get_amount() == CHUNK_SIZE:
                    message = json.dumps(self.data.to_json())
                    send_msg(self.socket,message)
                    self.data.set_fragments([])

    def _send_file(self, file_path: str, columns_to_send:  List[int]):
        with open(file_path, 'r') as data_file:
            reader = csv.reader(data_file)
            while True:
                data_chunk = self.read_chunk_with_columns(reader,columns_to_send)
                if not data_chunk or self._stop:
                    return
                self._send_data_chunk(data_chunk)
    
    def _send_last(self):
        fragment = DataFragment(self._queries.copy(), None , None)
        fragment.set_as_last()
        self.data.add_fragment(fragment)
        send_msg(self.socket, json.dumps(self.data.to_json()))
        self.data.set_fragments([])


    def _send_all_data_files(self):
        logger.info(f"Starting to send data, please wait")
        self._send_file(self._data_path + "/" + BOOKS_FILE_NAME, BOOKS_RELEVANT_COLUMNS)
        logger.info(f"All books have been sended")
        if any(query in self._queries for query in [3, 4, 5]):
            self._send_file(self._data_path + "/" + REVIEWS_FILE_NAME, REVIEWS_RELEVANT_COLUMNS)
            logger.info(f"All reviews have been sended")
        self._send_last()

    def run(self):
        self._event = Event()
        results_proccess = Process(target=self._handle_results, args=(self._event,))
        results_proccess.start()
        keys = ','.join([str(key) for key in self._queries.keys()])
        send_msg(self.socket,keys)
        
        self._send_all_data_files()
        logger.info(f"Data was submitted successfully, please wait for results")
        
        results_proccess.join()
    
    # Creates a result array
    # ['Query','Title','Author','Publisher','Publised Year','Categories','Distinc Amount', 'Average', 'Sentiment', 'Percentile']
    def get_result_from_datafragment(self, fragment: DataFragment) -> List[str]:
        book_result = [""] * 5
        query_info_results = [""] * 4
        query = str(list(fragment.get_queries().keys())[0])
        book = fragment.get_book()
        if book:
            book_result = book.get_result()
        query_info = fragment.get_query_info()
        if query_info:
            if book_result[1] == "":
                book_result[1] = query_info.get_author()
            query_info_results = query_info.get_result()

        return [query] + book_result + query_info_results

    def _handle_results(self, event):
        amount_of_queries_left = len(self._queries)
        with open(self._data_path + "/data/" + RESULTS_FILE_NAME, 'w', newline='') as result_file:
            writer = csv.writer(result_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(RESULTS_COLUMNS)
            while not event.is_set():
                chunk_msg = receive_msg(self.socket)
                json_chunk_msg = json.loads(chunk_msg)
                chunk = DataChunk.from_json(json_chunk_msg)
                for fragment in chunk.get_fragments():
                    if fragment.is_last():
                        amount_of_queries_left -= 1
                        continue
                    result = self.get_result_from_datafragment(fragment)
                    writer.writerow(result)
                if amount_of_queries_left <= 0:
                    break      
        logger.info(f"All queries have been processed")
        self.socket.close()