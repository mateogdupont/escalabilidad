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
    
    def add_and_try_to_send_chunk(self,fragment: DataFragment, node):
        if node == 'filter':
            self.clean_data_to_filter.append(fragment)
            if len(self.clean_data_to_filter) == MAX_AMOUNT_OF_FRAGMENTS or fragment.is_last():
                data_chunk = DataChunk(self.clean_data_to_filter)
                self.mom.publish(data_chunk, node)
                self.clean_data_to_filter = []

        elif node == 'counter':
            self.clean_data_to_counter.append(fragment)
            if len(self.clean_data_to_counter) == MAX_AMOUNT_OF_FRAGMENTS or fragment.is_last():
                data_chunk = DataChunk(self.clean_data_to_counter)
                self.mom.publish(data_chunk, node)
                self.clean_data_to_counter = []
        else:
            self.clean_data_to_sentiment.append(fragment)
            if len(self.clean_data_to_sentiment) == MAX_AMOUNT_OF_FRAGMENTS or fragment.is_last():
                data_chunk = DataChunk(self.clean_data_to_sentiment)
                self.mom.publish(data_chunk, node)
                self.clean_data_to_sentiment = []
        

    def send_clean_data(self, chunk_data: DataChunk):
        #self.clean_data_to_filter.append(fragment)
        #self.mom.publish(update_data_fragment_step(chunk))
        for fragment in chunk_data.get_fragments():
            #TODO: Filter client data
            dic = update_data_fragment_step(fragment)
            self.mom.publish(dic)
            # next_node_key = update_data_fragment_step(fragment).values()
            # self.add_and_try_to_send_chunk(fragment, next_node_key)

    def has_minimun_data(self, fragment: DataFragment):
        book = fragment.get_book()
        review = fragment.get_review()
        if book is not None:
            return book.has_minimun_data()
        elif review is not None:
            return review.has_minimun_data()
        else:
            return fragment.is_last()

    def clear_data(self, chunk: DataChunk):
        print("Voy a limpiar")
        filters_fragments = filter(self.has_minimun_data, chunk.get_fragments())
        chunk.set_fragments(list(filters_fragments))
        

    def run(self):
        socket = self._socket.accept()[0]
        while not self._exit:
            chunk_msg = receive_msg(socket)
            json_chunk_msg = json.loads(chunk_msg)
            chunk = DataChunk.from_json(json_chunk_msg)
            print(f"El chunk es {chunk.to_json()}")

            self.clear_data(chunk)

            self.send_clean_data(DataChunk.from_json(json_chunk_msg))
            if chunk.contains_last_fragment():
                print(f"All data was received")
                send_msg(socket,chunk.to_json())
                self._exit = True
            self._exit = True



def main():
    cleaner = DataCleaner()
    cleaner.run()
   
if __name__ == "__main__":
    main()