import socket
import signal
import os
import re
from typing import List, Tuple
from multiprocessing import Process, Event
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.structs.data_chunk import *
from utils.stream_communications import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step
from dotenv import load_dotenv # type: ignore
import sys
import time
import logging as logger

year_regex = re.compile('[^\d]*(\d{4})[^\d]*')

MAX_AMOUNT_OF_FRAGMENTS = 250
LISTEN_BACKLOG = 5
PORT = 1250

class DataCleaner:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        load_dotenv()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(('', 1250))
        self._socket.listen(LISTEN_BACKLOG)
        self.exit = False
        self._event = None
        self.queries = {}
        self.total_pass = 0
        self.clean_data = {}
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.work_queue = list(consumer_queues.keys())[0]
        self.mom = MOM(consumer_queues)
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)
    
    def sigterm_handler(self, signal,frame):
        self._socket.close()
        self.mom.close()
        self.exit = True
        if self._event:
            self._event.set()
    
    def add_and_try_to_send(self, fragment: DataFragment, node: str):
        self.clean_data[node] = self.clean_data.get(node, [])
        self.clean_data[node].append(fragment)
        if len(self.clean_data[node]) == MAX_AMOUNT_OF_FRAGMENTS or fragment.is_last():
            data_chunk = DataChunk(self.clean_data[node])
            self.mom.publish(data_chunk, node)
            self.clean_data[node].clear()
        
    def parse_year(self,read_date: str):
        if read_date:
            result = year_regex.search(read_date)
            return result.group(1) if result else None
        return None
    # Book db:
    # Title|description|authors|image|previewLink|publisher|pubishedDate|infoLink|categories|ratingCount
    # 0    1    2      3            4        5   6              7           8           9   10          11
    # last|book|Title|description|authors|image|previewLink|publisher|pubishedDate|infoLink|categories|ratingCount
    def create_book_fragment(self, unparsed_data):
        publish_year = self.parse_year(unparsed_data[8])
        book = Book(unparsed_data[2],"",unparsed_data[4],None,None,unparsed_data[7],publish_year,None,unparsed_data[10],unparsed_data[11])
        if book.has_minimun_data():
            return DataFragment(self.queries.copy(), book , None)
        else:
            return None


    # Reviews db:
    # 0    1    2   3       4   5       6                   7               8           9       10                  11
    # last|book|Id|Title|Price|User_id|profileName|review/helpfulness|review/score|review/time|review/summary|review/text
    def create_review_fragment(self, unparsed_data) -> DataFragment:
        review = Review(unparsed_data[2],unparsed_data[3],None,None,unparsed_data[7],unparsed_data[8],None,unparsed_data[10],unparsed_data[11])
        if review.has_minimun_data():
            return DataFragment(self.queries.copy(), None , review)
        else:
            return None
    
    def parse_and_filter_data(self, unparsed_data):
        if unparsed_data[1] == 1:
            return self.create_book_fragment(unparsed_data)
        else:
            return self.create_review_fragment(unparsed_data)

    def clear_and_try_to_send_data(self, unparsed_data_chunk) -> Tuple[int,bool]:
        amount_clean_fragments = 0
        last = False
        for data in unparsed_data_chunk:
            if self.exit:
                if self._event:
                    self._event.set()
                return (0,False)

            fragment = self.parse_and_filter_data(data)
            if fragment:
                amount_clean_fragments += 1
                self.total_pass += 1
                for value, key in update_data_fragment_step(fragment).items():
                    self.add_and_try_to_send(value, key)
            
            if data[0] == 1:
                if data[1] == 1:
                    logger.info(f"Me llego el ultimo libro :)")
                    book = Book("Last","",["Last"],None,None,"Last","2000",None,["Last"],0.0)
                    last_fragment = DataFragment(self.queries.copy(),book,None)  
                else:
                    logger.info(f"Me llego la ultima review :)")
                    review = Review(0,"Last",None,None,"Last",1.0,None,"Last","Last")
                    last_fragment = DataFragment(self.queries.copy(),None,review)
                last = True
                last_fragment.set_as_last()
                for value, key in update_data_fragment_step(last_fragment).items():
                    self.add_and_try_to_send(value, key)

        return (amount_clean_fragments,last)

    def receive_and_try_to_send_clean_data(self, client_socket) -> Tuple[int, bool]:
        while not self.exit:
            try:
                data_msg = receive_msg(client_socket)
                if not data_msg:
                    return (0,False)
                return self.clear_and_try_to_send_data(data_msg)
            except socket.error as e:
                logger.info(f"Error en el socket: {e}")
                return (0,False)

    def receive_files(self,client_socket, expected_amount_of_files):
        finish = False
        while not self.exit and not finish:
            (amount, last) = self.receive_and_try_to_send_clean_data(client_socket)
            if amount == 0:
                finish = True
                if self._event:
                    self._event.set()
                break
            if last:
                expected_amount_of_files -= 1
                logger.info(f"Me llego un amount y espero {expected_amount_of_files}")
                if expected_amount_of_files == 0:
                    finish = True
        return expected_amount_of_files
    
    def handle_client(self, client_socket):
        try:
            queries = receive_msg(client_socket)
            self.queries = {int(key): 0 for key in queries}
        except socket.timeout:
                logger.info(f"Client didn't answer in time")
        self._event = Event()
        results_proccess = Process(target=self._send_results, args=(client_socket,queries,self._event,))
        results_proccess.start()
        expected_amount_of_files =  2 if any(query in self.queries for query in [3, 4, 5]) else 1
        remainding_amount = self.receive_files(client_socket, expected_amount_of_files)
        if remainding_amount == 0:
            logger.info(f"All data was received: {self.total_pass}")
        results_proccess.join()
        client_socket.close()

    def run(self):
        while not self.exit:
            try: 
                socket = self._socket.accept()[0]
                self.handle_client(socket)
            except OSError as err:
                logger.info(f"Error in socket: {err}")
                

    def _send_results(self,socket,queries,event):
        queries_left = len(queries)

        while not event.is_set() and queries_left > 0:
            msg = self.mom.consume(self.work_queue)
            if not msg:
                time.sleep(2)
                continue
            (data_chunk, tag) = msg
            message = json.dumps(data_chunk.to_json())
            send_msg(socket,message)
            if data_chunk.contains_last_fragment():
                logger.info(f"Last - Data chunk: {data_chunk.to_json()}")
                queries_left -= 1
            self.mom.ack(tag)
        if not event.is_set():
            logger.info(f"All results has been delivered.")



def main():
    cleaner = DataCleaner()
    cleaner.run()
    if not cleaner.exit:
        cleaner.mom.close()
   
if __name__ == "__main__":
    main()