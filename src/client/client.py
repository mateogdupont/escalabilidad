import csv
import signal
import datetime
from multiprocessing import Process
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *


CHUNK_SIZE = 100
BOOKS_FILE_NAME = "books_data.csv"
REVIEWS_FILE_NAME = "Books_rating.csv"
RESULTS_FILE_NAME = "Results.csv"
BOOKS_RELEVANT_COLUMNS = [0,1,2,5,6,8,9]
REVIEWS_RELEVANT_COLUMNS = [0,1,5,6,8,9]
BOOKS_ARGUMENT_AMOUNT = 7
REVIEW_ARGUMENT_AMOUNT = 6

# Book db:
# Title|description|authors|image|previewLink|publisher|pubishedDate|infoLink|categories|ratingCount

# Reviews db:
# Id|Title|Price|User_id|profileName|review/helpfulness|review/score|review/time|review/summary|review/text

class Client:
    def __init__(self, data_path, queries):
        self._data_path = data_path
        self._queries = queries
        self._stop = False
        self.total = 0
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def sigterm_handler(self, signal,frame):
        self._stop = True

    def read_chunk_with_columns(self,reader, columns):
        chunk = []
        for row in reader:
            element = [row[i] for i in columns]
            chunk.append(element)
            if len(chunk) == CHUNK_SIZE or self._stop:
                return chunk
        return chunk

    def _send_file(self, file_path, columns_to_send):
        with open(file_path, 'r') as data_file:
            reader = csv.reader(data_file)
            while True:
                data_chunk = self.read_chunk_with_columns(reader,columns_to_send)
                if not data_chunk or self._stop:
                    return
                self._send_data_chunk(data_chunk)

    def _send_all_data_files(self):
        print("Starting to send data, please wait")
        self._send_file(self._data_path + "/" + BOOKS_FILE_NAME, BOOKS_RELEVANT_COLUMNS)
        self._send_file(self._data_path + "/" + REVIEWS_FILE_NAME, REVIEWS_RELEVANT_COLUMNS)

    def parse_data(self, data):
        book = None
        review = None
        if len(data) == BOOKS_ARGUMENT_AMOUNT:
            book = Book(data[0],data[1],data[2],None,None,data[3],data[4],None,data[5],data[6])
        elif len(data) == REVIEW_ARGUMENT_AMOUNT:
            review = Review(data[0],data[1],None,None,data[2],data[3],None,data[4],data[5])
        else:
            return None
        return DataFragment(self._queries, 0, book , review)
        
    def _send_data_chunk(self,data_chunk):
        for data in data_chunk:
            parsed_data = self.parse_data(data)
            if parsed_data != None:
                print(f"Send data: Title: {parsed_data.get_book().get_title()}, Authors: {parsed_data.get_book().get_authors()}")
                #TODO: Send data using MOM
                #publish(parsed_data)

    def run(self):
        results_proccess = Process(target=self._handle_results, args=())
        results_proccess.start()

        try:
            self._send_all_data_files()
        except Exception as err:
            print(f"Error sending data files: {err}")
        print("Data was submitted successfully, please wait for results")
        
        results_proccess.join()

    #TODO Read DataFragment and save it in CSV
    def _handle_results(self):
        print("All queries have been processed")