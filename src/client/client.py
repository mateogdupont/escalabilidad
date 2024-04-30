import csv
import signal
import os
import re
from typing import List
import datetime
from multiprocessing import Process, Event
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.structs.data_chunk import *
from utils.stream_communications import *
from utils.query_updater import update_data_fragment_step
from dotenv import load_dotenv

year_regex = re.compile('[^\d]*(\d{4})[^\d]*')

CHUNK_SIZE = 100
BOOKS_FILE_NAME = "books_data.csv"
REVIEWS_FILE_NAME = "Books_rating.csv"
RESULTS_FILE_NAME = "Results.csv"
RESULTS_COLUMNS = ['Query','Author','Distinc Amount', 'Average', 'Sentiment', 'Percentile']
BOOKS_RELEVANT_COLUMNS = [0,1,2,5,6,8,9]
REVIEWS_RELEVANT_COLUMNS = [0,1,5,6,8,9]
BOOKS_ARGUMENT_AMOUNT = 7
REVIEW_ARGUMENT_AMOUNT = 6

# Book db:
# Title|description|authors|image|previewLink|publisher|pubishedDate|infoLink|categories|ratingCount

# Reviews db:
# Id|Title|Price|User_id|profileName|review/helpfulness|review/score|review/time|review/summary|review/text

class Client:
    def __init__(self, data_path: str, queries: dict[int, int], socket):
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
        print("Starting to send data, please wait")
        self._send_file(self._data_path + "/" + BOOKS_FILE_NAME, BOOKS_RELEVANT_COLUMNS)
        #self._send_file(self._data_path + "/" + REVIEWS_FILE_NAME, REVIEWS_RELEVANT_COLUMNS)
        self._send_last()

    def run(self):
        self._event = Event()
        results_proccess = Process(target=self._handle_results, args=(self._event,))
        results_proccess.start()

        # try:
        self._send_all_data_files()
        # except Exception as err:
        #     print(f"Error sending data files: {err}")
        print("Data was submitted successfully, please wait for results")
        
        results_proccess.join()

    #TODO Parse and save results in CSV
    def _handle_results(self, event):
        while True:
            msg = self.socket.recv(1024).decode('utf-8')
            if '|' in msg:
                break
        print(f"Me llego el msg de confirmacion como: {msg}")
        self.socket.close()
        # with open(RESULTS_FILE_NAME, 'w', newline='') as csvfile:
        #     writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        #     writer.writerows(RESULTS_COLUMNS)
        #     while not event.is_set():
        #         (data_fragment_chunk, tag) = self.mom.consume("results")
        #         print(f"Write results {data_fragment_chunk}")
        #         self.mom.ack(tag)

        # print("All queries have been processed")