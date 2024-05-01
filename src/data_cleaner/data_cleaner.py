import socket
import signal
import os
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.structs.data_chunk import *
from utils.stream_communications import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step
from dotenv import load_dotenv
import sys
import logging as logger

MAX_AMOUNT_OF_FRAGMENTS = 100
LISTEN_BACKLOG = 5
PORT = 1250

class DataCleaner:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        load_dotenv()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(('', 1250))
        self._socket.listen(LISTEN_BACKLOG)
        self._exit = False
        self.total_pass = 0
        self.clean_data = {}
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.work_queue = list(consumer_queues.keys())[0]
        self.mom = MOM(consumer_queues)
        signal.signal(signal.SIGTERM, self.sigterm_handler)
    
    def sigterm_handler(self):
        self._exit = True
    
    def add_and_try_to_send_chunk(self, fragment: DataFragment, node: str):
        # # self.clean_data[node] = self.clean_data.get(node, []).append(fragment)
        # if not node in self.clean_data:
        #     self.clean_data[node] = []
        self.clean_data[node] = self.clean_data.get(node, [])
        self.clean_data[node].append(fragment)
        if len(self.clean_data[node]) == MAX_AMOUNT_OF_FRAGMENTS:
            data_chunk = DataChunk(self.clean_data[node])
            self.mom.publish(data_chunk, node)
            self.clean_data[node].clear()
            
    def update_last_and_send_chunk(self):
        for node in self.clean_data.keys():
            if len(self.clean_data[node]) == 0:
                continue
            self.clean_data[node][-1].set_as_last()
            data_chunk = DataChunk(self.clean_data[node])
            self.mom.publish(data_chunk, node)
            self.clean_data[node].clear()
        

    def send_clean_data(self, chunk_data: DataChunk):
        for fragment in chunk_data.get_fragments():
            for data, key in update_data_fragment_step(fragment).items():
                self.add_and_try_to_send_chunk(data, key)
            if fragment.is_last():
                self.update_last_and_send_chunk()

            
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
        

    def run(self):
        socket = self._socket.accept()[0]
        while not self._exit:
            chunk_msg = receive_msg(socket)
            json_chunk_msg = json.loads(chunk_msg)
            chunk = DataChunk.from_json(json_chunk_msg)
            self.clear_data(chunk)
            self.send_clean_data(chunk)
            if chunk.contains_last_fragment():
                print(f"All data was received: {self.total_pass}")
                send_msg(socket,json.dumps(chunk.to_json()))
                self._exit = True



def main():
    cleaner = DataCleaner()
    cleaner.run()
   
if __name__ == "__main__":
    main()