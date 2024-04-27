import socket
import signal
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step

MAX_AMOUNT_OF_FRAGMENTS = 100
LISTEN_BACKLOG = 5
PORT = 1250

class DataCleaner:
    def __init__(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(('', 1250))
        self._socket.listen(LISTEN_BACKLOG)
        self._exit = False
        self.clean_data_to_filter = []
        self.clean_data_to_counter = []
        self.clean_data_to_sentiment = []
        self.mom = MOM({'filter':None,'counter':None,'sentiment_analysis':None})
        signal.signal(signal.SIGTERM, self.sigterm_handler)
    
    def sigterm_handler(self):
        self._exit = True

    def receive_chunk_header(self, client_sock):
        complete_header = ""
        while True:
            partial_header = client_sock.recv(1).decode('utf-8')
            if not partial_header:
                print("Error reading chunk header")
                break
            complete_header += partial_header
            if partial_header == '|':
                break
        return complete_header


    def receive_chunk(self, client_sock):
        header = self.receive_chunk_header(client_sock)
        if not header:
            return ""
        expected_payload_size = int(header[:-1])
        complete_msg = header
        while True:
            partial_msg = client_sock.recv(expected_payload_size).decode('utf-8')
            if not partial_msg:
                print("Error reading chunk")
                break

            complete_msg += partial_msg
            if '|' in complete_msg:
                split_msg = complete_msg.split('|', 1)
                received_payload_size = len(split_msg[1])
                if received_payload_size >= expected_payload_size:
                    break
        return complete_msg.split('|', 1)[1]
    
    def add_and_try_to_send_chunk(self,fragment, node):
        if node == 'filter':
            self.clean_data_to_filter.append(fragment)
            if len(self.clean_data_to_filter) == MAX_AMOUNT_OF_FRAGMENTS:
                data_chunk = DataFragmentChunk(self.clean_data_to_filter)
                self.mom.publish(node,data_chunk)
                self.clean_data_to_filter = []

        elif node == 'counter':
            self.clean_data_to_counter.append(fragment)
            if len(self.clean_data_to_counter) == MAX_AMOUNT_OF_FRAGMENTS:
                data_chunk = DataFragmentChunk(self.clean_data_to_counter)
                self.mom.publish(node,data_chunk)
                self.clean_data_to_counter = []
        else:
            self.clean_data_to_sentiment.append(fragment)
            if len(self.clean_data_to_sentiment) == MAX_AMOUNT_OF_FRAGMENTS:
                data_chunk = DataFragmentChunk(self.clean_data_to_sentiment)
                self.mom.publish(node,data_chunk)
                self.clean_data_to_sentiment = []
        

    def send_clean_data(self, chunk_msg):
        for msg in chunk_msg:
            #TODO: Filter client data
            fragment = DataFragment.from_json(msg)
            next_node_key = update_data_fragment_step(fragment).values()
            self.add_and_try_to_send_chunk(fragment, next_node_key)


    def run(self, socket):
        socket = self._socket.accept()
        while not self._exit:
            chunk_msg = self.receive_chunk(socket)
            json_chunk_msg = json.loads(chunk_msg)
            self.send_clean_data(json_chunk_msg)



def main():
    cleaner = DataCleaner()
    cleaner.run()
   
if __name__ == "__main__":
    main()