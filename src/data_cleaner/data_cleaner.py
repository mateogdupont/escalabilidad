import socket
import signal
import os
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.stream_communications import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step
from dotenv import load_dotenv

MAX_AMOUNT_OF_FRAGMENTS = 100
LISTEN_BACKLOG = 5
PORT = 1250

class DataCleaner:
    def __init__(self):
        load_dotenv()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(('', 1250))
        self._socket.listen(LISTEN_BACKLOG)
        self._exit = False
        self.clean_data_to_filter = []
        self.clean_data_to_counter = []
        self.clean_data_to_sentiment = []
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.work_queue = list(consumer_queues.keys())[0]
        self.mom = MOM(consumer_queues)
        signal.signal(signal.SIGTERM, self.sigterm_handler)
    
    def sigterm_handler(self):
        self._exit = True
    
    def add_and_try_to_send_chunk(self,fragment, node):
        pass
        # if node == 'filter':
        #     self.clean_data_to_filter.append(fragment)
        #     if len(self.clean_data_to_filter) == MAX_AMOUNT_OF_FRAGMENTS:
        #         data_chunk = DataFragmentChunk(self.clean_data_to_filter)
        #         self.mom.publish(node,data_chunk)
        #         self.clean_data_to_filter = []

        # elif node == 'counter':
        #     self.clean_data_to_counter.append(fragment)
        #     if len(self.clean_data_to_counter) == MAX_AMOUNT_OF_FRAGMENTS:
        #         data_chunk = DataFragmentChunk(self.clean_data_to_counter)
        #         self.mom.publish(node,data_chunk)
        #         self.clean_data_to_counter = []
        # else:
        #     self.clean_data_to_sentiment.append(fragment)
        #     if len(self.clean_data_to_sentiment) == MAX_AMOUNT_OF_FRAGMENTS:
        #         data_chunk = DataFragmentChunk(self.clean_data_to_sentiment)
        #         self.mom.publish(node,data_chunk)
        #         self.clean_data_to_sentiment = []
        

    def send_clean_data(self, chunk_msg):
        fragment = DataFragment.from_json(chunk_msg)
        self.clean_data_to_filter.append(fragment)
        self.mom.publish(update_data_fragment_step(fragment))
        # for msg in chunk_msg:
        #     #TODO: Filter client data
        #     fragment = DataFragment.from_json(msg)
        #     next_node_key = update_data_fragment_step(fragment).values()
        #     self.add_and_try_to_cd_chunk(fragment, next_node_key)


    def run(self):
        socket = self._socket.accept()[0]
        while not self._exit:
            chunk_msg = receive_msg(socket)
            #json_chunk_msg = json.loads(chunk_msg)
            #self.send_clean_data(json_chunk_msg)
            fragment = DataFragment.from_json(chunk_msg)
            if fragment.is_last():
                print(f"All data was received")
                send_msg(socket,fragment.to_json())
                self._exit = True
            else: 
                self.send_clean_data(chunk_msg)



def main():
    cleaner = DataCleaner()
    cleaner.run()
   
if __name__ == "__main__":
    main()