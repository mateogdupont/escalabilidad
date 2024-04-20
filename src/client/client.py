import csv
import signal
from multiprocessing import Process

CHUNK_SIZE = 100
BOOKS_FILE_NAME = "Books_rating.csv"
REVIEWS_FILE_NAME = "books_data.csv"
RESULTS_FILE_NAME = "Results.csv"
BOOKS_RELEVANT_COLUMNS = [0,1,2,5,6,8,9]
REVIEWS_RELEVANT_COLUMNS = [0,1,5,6,8,9]

# Book db:
# Title|description|authors|image|previewLink|publisher|pubishedDate|infoLink|categories|ratingCount

# Reviews db:
# Id|Title|Price|User_id|profileName|review/helpfulness|review/score|review/time|review/summary|review/text

class Client:
    def __init__(self, data_path, queries):
        self._data_path = data_path
        self._queries = queries
        self._stop = False
        signal.signal(signal.SIGTERM, self.sigterm_handler)

    def sigterm_handler(self, signal,frame):
        self._stop = True

    def read_chunk_with_columns(self,reader, columns):
        chunk = []
        for row in reader:
            chunk.append([row[i] for i in columns])
            if len(chunk) == CHUNK_SIZE or self._stop:
                return chunk
        return chunk

    def _send_file(self, file_path, columns_to_send):
        with open(file_path, 'r') as data_file:
            reader = csv.reader(data_file)
            data_chunk = self.read_chunk_with_columns(reader,columns_to_send)
            if not data_chunk or self._stop:
                return
            send_chunk(data_chunk)

    def run(self):
        results_proccess = Process(target=results_handler, args=())
        print("Starting to send data, please wait")
        
        try:
            self._send_file(self._data_path + "/" + BOOKS_FILE_NAME, BOOKS_RELEVANT_COLUMNS)
            self._send_file(self._data_path + "/" + REVIEWS_FILE_NAME, REVIEWS_RELEVANT_COLUMNS)
        except Exception as err:
            print(f"Error sending data files: {err}")
        print("Data was submitted successfully, please wait for results")
        
        results_proccess.join()
        print("All queries have been processed")

#TODO: Create and send a message with the required columns to RabbitMQ
def send_chunk(chunk):
    for msg in chunk:
        send(msg)

#TODO Read from RabbitMQ and save it in CSV
def results_handler():
    print("TODO")