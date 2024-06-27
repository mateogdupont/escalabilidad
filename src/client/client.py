import csv
import signal
import os
import socket
import time
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

CHUNK_SIZE = 800
BOOKS_FILE_NAME = "books_data.csv"
REVIEWS_FILE_NAME = "Books_rating.csv"
RESULTS_FILE_NAME = "Results"
RESULTS_COLUMNS = [['Title','Author','Publisher','Publised Year','Categories'],
                   ['Author','Distinc Amount'],
                   ['Title','Author','Publisher','Publised Year','Categories','Distinc Amount', 'Average'],
                   ['Title','Author','Publisher','Publised Year','Categories','Distinc Amount', 'Average'],
                   ['Title','Author','Publisher','Publised Year','Categories','Sentiment', 'Percentile']]
# Message:
# last_bool|book_bool|column_1|column_2|...|column_n

# Book db:
# Title|description|authors|image|previewLink|publisher|pubishedDate|infoLink|categories|ratingCount

# Reviews db:
# Id|Title|Price|User_id|profileName|review/helpfulness|review/score|review/time|review/summary|review/text

class Client:
    def __init__(self, data_path: str, queries: 'dict[int, int]', socket):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        load_dotenv()
        self.id = os.environ["ID"]
        self._data_path = data_path
        self._queries = queries
        self._stop = False
        self._event = None
        self.total = 0
        self.socket = socket
        self.data = []
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)

    def sigterm_handler(self, signal,frame):
        self._stop = True
        if self._event:
            self._event.set()
        
    def read_chunk(self,reader, is_book_file: bool) -> int:
        self.data = []
        for row in reader:
            self.data.append([0, int(is_book_file)] + row)
            if len(self.data) == CHUNK_SIZE or self._stop:
                break

    def _send_file(self, file_path: str, is_book_file: bool):
        try:
            logger.info(f"Starting to send: {file_path}")
            previus_read_data = []
            with open(file_path, 'r') as data_file:
                reader = csv.reader(data_file)
                first = next(reader)

                while True and not self._stop:
                    self.read_chunk(reader, is_book_file)
                    if not self.data or self._stop:
                        if len(previus_read_data) > 0:
                            previus_read_data[len(previus_read_data) - 1][0] = 1
                            send_msg(self.socket,previus_read_data)
                        break
                    if len(previus_read_data) > 0:
                        send_msg(self.socket,previus_read_data)
                    previus_read_data = self.data
                    
            if not self._stop:
                logger.info(f"All data in {file_path} have been sended")
        except socket.error as e:
            logger.error("Error 599: Network connect timeout error, try again later please") # https://http.cat/status/599



    def _send_all_data_files(self):
        self._send_file(self._data_path + "/data/" + BOOKS_FILE_NAME, True)
        if any(query in self._queries for query in [3, 4, 5]):
            self._send_file(self._data_path + "/data/" + REVIEWS_FILE_NAME, False)

    def run(self):
        self._event = Event()
        results_proccess = Process(target=self._handle_results, args=(self._event,))
        results_proccess.start()
        keys = list(self._queries.keys())
        try:
            send_msg(self.socket,keys)
        except socket.error as e:
            logger.error("Error 599: Network connect timeout error, try again later please") # https://http.cat/status/599
        logger.info(f"Starting to send data, please wait") 
        self._send_all_data_files()
        if not self._stop:
            logger.info(f"Data was submitted successfully, please wait for results")
        else:
            logger.info(f"Operation was aborted due to a termination signal.")
        results_proccess.join()
    
    def receive_result(self, event) -> DataChunk:
        while not event.is_set():
            try:
                chunk_msg = receive_msg(self.socket)
                return chunk_msg
            except socket.error as e:
                logger.error("Error 599: Network connect timeout error, try again later please") # https://http.cat/status/599
                return None
            
    def _initialize_result_files_and_writers(self):
        result_files = {}
        result_writers = {}
        for i in self._queries.keys():
            result_file = open(self._data_path + "/data/" + RESULTS_FILE_NAME + "_" + str(i) + "_id_" + self.id + ".csv", 'w', newline='')
            # result_writers.append(csv.writer(result_file, delimiter=',', quoting=csv.QUOTE_MINIMAL))
            result_writers[i] = csv.writer(result_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
            result_writers[i].writerow(RESULTS_COLUMNS[i-1])
            result_files[i] = result_file
        return (result_files,result_writers)

    def write_result_in_file(self, result_writers, result):
        query = int(result[1])
        if query == 1:
            result_index = [2,3,4,5,6]
        elif query == 2:
            result_index = [3,7]
        elif query == 3 or query == 4:
            result_index = [2,3,4,5,6,7,8]
        else:
            result_index = [2,3,4,5,6,9,10]
        rows = [result[i] for i in result_index]
        try:
            result_writers[query].writerow(rows)
        except Exception as e:
            logger.info(f"Error writing in file: {e}")
            logger.info(f"Query: {query}")
            logger.info(f"rows: {rows}")

# ['last','Query','Title','Author','Publisher','Publised Year','Categories','Distinc Amount', 'Average', 'Sentiment', 'Percentile']
    def _handle_results(self, event):
        amount_of_queries_left = len(self._queries)
        (result_files,result_writers) = self._initialize_result_files_and_writers()

        while not event.is_set():
            results = self.receive_result(event)
            if not results:
                break
            for result in results:
                if result[0] == '1':
                    amount_of_queries_left -= 1
                    result_files[int(result[1])].close()
                    logger.info(f"The query {result[1]} has been processed")
                    continue
                self.write_result_in_file(result_writers, result)
                if event.is_set():
                    break
            if amount_of_queries_left <= 0:
                break
        if not event.is_set():
            for file in result_files.values():
                file.close()
            logger.info(f"All queries have been processed")
        self.socket.close()