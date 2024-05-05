import socket
import signal
import os
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

MAX_AMOUNT_OF_FRAGMENTS = 500
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
    
    def add_and_try_to_send_chunk(self, fragment: DataFragment, node: str):
        self.clean_data[node] = self.clean_data.get(node, [])
        self.clean_data[node].append(fragment)
        if len(self.clean_data[node]) == MAX_AMOUNT_OF_FRAGMENTS or fragment.is_last():
            data_chunk = DataChunk(self.clean_data[node])
            self.mom.publish(data_chunk, node)
            self.clean_data[node].clear()

    def send_clean_data(self, chunk_data: DataChunk):
        for fragment in chunk_data.get_fragments():
            if self.exit:
                if self._event:
                    self._event.set()
                return
            if fragment.is_last():
                logger.info(f"Last fragment received")
            for data, key in update_data_fragment_step(fragment).items():
                self.add_and_try_to_send_chunk(data, key)
            
    def has_minimun_data(self, fragment: DataFragment):
        book = fragment.get_book()
        review = fragment.get_review()
        if book is not None:
            return book.has_minimun_data()
        elif review is not None:
            return review.has_minimun_data()
        else:
            return fragment.is_last()

    def clear_data(self, chunk: DataChunk) -> DataChunk:
        filters_fragments = list(filter(self.has_minimun_data, chunk.get_fragments()))
        self.total_pass += len(filters_fragments)
        chunk.set_fragments(filters_fragments)
        return chunk

    def try_to_receive_chunk(self, client_socket) -> DataChunk:
        while not self.exit:
            try:
                chunk_msg = receive_msg(client_socket)
                if not chunk_msg:
                    return None
                json_chunk_msg = json.loads(chunk_msg)
                return DataChunk.from_json(json_chunk_msg)
            except socket.timeout:
                time.sleep(1)
            except socket.error as e:
                logger.info(f"Error en el socket: {e}")
                return None

    def handle_client(self, client_socket):
        finish = False
        queries = receive_msg(client_socket)
        try:
            queries = receive_msg(client_socket)
        except socket.timeout:
                logger.info(f"Client didn't answer in time")
        self._event = Event()
        results_proccess = Process(target=self._send_results, args=(client_socket,queries,self._event,))
        results_proccess.start()
        while not self.exit and not finish:
            chunk = self.try_to_receive_chunk(client_socket)
            if not chunk:
                finish = True
                if self._event:
                    self._event.set()
                break
            self.clear_data(chunk)
            self.send_clean_data(chunk)
            if chunk.contains_last_fragment():
                logger.info(f"All data was received: {self.total_pass}")
                finish = True
        results_proccess.join()
        client_socket.close()

    def run(self):
        while not self.exit:
            try: 
                socket = self._socket.accept()[0]
                # socket.settimeout(1.0)
                self.handle_client(socket)
            except OSError as err:
                logger.info(f"Error in socket: {err}")
                

    def _send_results(self,socket,queries,event):
        queries_left = len(queries.split(','))
        while not event.is_set() and queries_left > 0:
            msg = self.mom.consume(self.work_queue)
            if not msg:
                time.sleep(2)
                continue
            (data_chunk, tag) = msg
            message = json.dumps(data_chunk.to_json())
            send_msg(socket,message)
            if data_chunk.contains_last_fragment():
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