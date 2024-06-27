import socket
import signal
import os
from typing import List, Tuple
from multiprocessing import Process, Event, Queue
from queue import Empty
import uuid
from log_manager.log_recoverer import LogRecoverer
from log_manager.log_writer import LogWriter
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

load_dotenv()
MAX_AMOUNT_OF_FRAGMENTS = 500
MAX_AMOUNT_OF_CLIENTS = 10
LISTEN_BACKLOG = 5
PORT = 1250
MEDIC_IP_ADDRESSES=eval(os.environ.get("MEDIC_IPS"))
MEDIC_PORT=int(os.environ["MEDIC_PORT"])
NODE_TYPE=os.environ["NODE_TYPE"]
HEARTBEAT_INTERVAL=int(os.environ["HEARTBEAT_INTERVAL"])

# TODO: clean log data somewhere

class DataCleaner:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        self.info_all_key = os.environ["INFO_KEY_ALL"]
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.bind(('', 1250))
        self._socket.listen(LISTEN_BACKLOG)
        self.exit = False
        self._event = None
        self.queries = {}
        self.clean_data = {}
        self.ignore_ids = set()
        self.work_queue = None
        self.mom = None
        self.id= os.environ["ID"]
        self.data_in_processes_queue = Queue()
        self.clients_to_results_queue = Queue()
        self.clients_processes = {}
        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)
        self.log_writer = LogWriter(os.environ["LOG_PATH"])
    
    def _initialice_mom(self):
        repr_consumer_queues = os.environ["CONSUMER_QUEUES"]
        consumer_queues = eval(repr_consumer_queues)
        self.work_queue = consumer_queues[0]
        self.mom = MOM(consumer_queues)

    def manage_clients(self):
        log_recoverer = LogRecoverer(os.environ["LOG_PATH"])
        log_recoverer.recover_data()
        previous_clients = log_recoverer.get_clients()
        logger.info(f"Previous clients: {previous_clients}")
        for client in previous_clients:
            self.send_clean_flag(client)
            logger.info(f"Sent clean flag about {client}")
    
    def sigterm_handler(self, signal,frame):
        self._socket.close()
        if self.mom:
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
    
    def send_clean_flag(self, client_id): # TODO: use this
        logger.info(f"Sending clean flag about {client_id}")
        self.ignore_ids.add(client_id)
        datafragment = DataFragment(0, {}, None, None, client_id)
        datafragment.set_as_clean_flag()
        self.mom.publish(datafragment, self.info_all_key)
        datachunk = DataChunk([datafragment])
        self.mom.publish(datachunk, "results")
        self.log_writer.log_ended_client(client_id)
        logger.info(f"Sent clean flag about {client_id}")
    
    def parse_and_filter_data(self, unparsed_data, client_id, next_id):
        if unparsed_data[1] == 1:
            return DataFragment.from_raw_book_data(next_id, unparsed_data, client_id,self.queries.copy())
        else:
            return DataFragment.from_raw_review_data(next_id, unparsed_data, client_id,self.queries.copy())

    def clear_and_try_to_send_data(self, unparsed_data_chunk, client_id, next_id) -> Tuple[int,bool]:
        amount_clean_fragments = 0
        last = False
        for data in unparsed_data_chunk:
            if self.exit:
                if self._event:
                    self._event.set()
                return (0,False, next_id)

            fragment = self.parse_and_filter_data(data, client_id, next_id)
            next_id += 1
            if fragment:
                amount_clean_fragments += 1
                review = fragment.get_review()
                for value, key in update_data_fragment_step(fragment).items():
                    if not 5 in fragment.get_queries() and review:
                        review.set_text("")
                        fragment.set_review(review)
                    self.add_and_try_to_send(value, key)
            
            if data[0] == 1:
                if data[1] == 1:
                    book = Book("Last",None,["Last"],None,None,"Last","2000",None,["Last"],0.0)
                    last_fragment = DataFragment(next_id, self.queries.copy(),book,None, client_id)  
                else:
                    review = Review(None,"Last",None,None,None,1.0,None,None,"Last")
                    last_fragment = DataFragment(next_id, self.queries.copy(),None,review, client_id)
                last = True
                last_fragment.set_as_last()
                next_id += 1
                for value, key in update_data_fragment_step(last_fragment).items():
                    self.add_and_try_to_send(value, key)

        return (amount_clean_fragments,last, next_id)

    def receive_and_try_to_send_clean_data(self, client_socket, client_id, next_id) -> Tuple[int, bool]:
        while not self.exit:
            try:
                data_msg = receive_msg(client_socket)
                if not data_msg:
                    return (0,False, next_id)
                return self.clear_and_try_to_send_data(data_msg, client_id, next_id)
            except socket.error as e:
                logger.info(f"Error en el socket: {e}")
                return (0,False, next_id)

    def receive_files(self,client_socket, expected_amount_of_files, client_id):
        finish = False
        next_id = 0
        while not self.exit and not finish:
            (amount, last, next_id) = self.receive_and_try_to_send_clean_data(client_socket, client_id, next_id)
            next_id += 1
            if amount == 0:
                finish = True
                if self._event:
                    self._event.set()
                break
            if last:
                expected_amount_of_files -= 1
                if expected_amount_of_files == 0:
                    finish = True
        return expected_amount_of_files
    
    def handle_client(self, client_socket,client_uuid):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        logger.info(f"Client connected with id: {client_uuid}")
        self.log_writer.log_new_client(client_uuid)
        try:
            queries = receive_msg(client_socket)
            self.queries = {int(key): 0 for key in queries}
        except Exception:
            logger.info(f"Client didn't answer in time")
            try:
                client_socket.close()
            except socket.error as e:
                pass # Se asume que el cliente ya se desconectó y se cerró el socket
            self.send_clean_flag(client_uuid)
            self.data_in_processes_queue.put(client_uuid)
            return
        msg_to_result_thread = [client_uuid, client_socket,len(queries)]
        self.clients_to_results_queue.put(msg_to_result_thread)
        self._initialice_mom()
        expected_amount_of_files =  2 if any(query in self.queries for query in [3, 4, 5]) else 1
        remainding_amount = self.receive_files(client_socket, expected_amount_of_files, client_uuid)
        if remainding_amount == 0:
            logger.info(f"All data was received")
            self.log_writer.log_ended_client(client_uuid)
        else:
            logger.info(f"Client didn't send all data")
            self.send_clean_flag(client_uuid)
        client_socket.close()               
        logger.info(f"Client disconnected with id: {client_uuid}")          
        self.data_in_processes_queue.put(client_uuid) 


    def try_clean_processes(self):
        while True:
            try:
                message = self.data_in_processes_queue.get(False)
                self.clients_processes[message].join()
                del self.clients_processes[message]
            except Empty:
                return

    def try_update_clients(self, clients):
        while True:
            try:
                # logger.info(f"Antes de pedir a {self.clients_to_results_queue}")
                message = self.clients_to_results_queue.get(False)
                # logger.info(f"Despues de pedir")
                clients[message[0]] = message[1:]
            except Empty:
                return
   
    def proccess_result_chunk(self,event,clients, data_chunk, received_ids):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        # logger.info(f"Entre a process")
        for fragment in data_chunk.get_fragments(): # agregar que se envíe a la results el flag de clean y manejarlo acá
            # logger.info(f"Entre a process con fragment")
            if event.is_set():
                break
            if not save_id(received_ids, fragment):
                # logger.info(f"3: No save id {received_ids}")
                continue
                
            client_id = fragment.get_client_id()
            if client_id not in clients or client_id in self.ignore_ids:
                logger.info(f"Read result for unregistered client")
                continue
            socket = clients.get(client_id)[0]
            if fragment.get_query_info().is_clean_flag():
                try:
                    socket.close()
                except socket.error as e:
                    pass # Se asume que el cliente ya se desconectó y se cerró el socket
                del clients[client_id]
                logger.info(f"Cleaning data from client {client_id}")
                continue
            # if not socket:
            #     logger.info(f"Read result for unregistered client")
            #     continue
            try:
                send_msg(socket,[fragment.to_result()])
            except Exception:
                logger.info(f"Error en el socket: Se asume que el cliente se desconectó")
                self.send_clean_flag(client_id)
                try:
                    socket.close()
                except socket.error as e:
                    pass # Se asume que el cliente ya se desconectó y se cerró el socket
                del clients[client_id]
                continue

            if fragment.is_last():
                logger.info(f"Last para {client_id}")
                queries_left = clients[client_id][1]
                if queries_left == 1:
                    # logger.info(f"Era el ultimo asique mato al client {client_id} en los results")
                    socket.close()
                    del clients[client_id]
                    self.send_clean_flag(client_id)
                else:
                    clients[client_id] = [socket, queries_left - 1]

    def results_handler(self,event):
        self._initialice_mom()
        self.manage_clients()
        clients = {}
        received_ids = {}
        while not event.is_set():
            self.try_update_clients(clients)
            msg = self.mom.consume(self.work_queue)
            if not msg:
                time.sleep(1)
                continue
            (data_chunk, tag) = msg
            self.proccess_result_chunk(event,clients,data_chunk, received_ids)
            self.mom.ack(tag)

    def run_cleaner(self, event):
        self._event = event
        results_proccess = Process(target=self.results_handler, args=(self._event,))
        results_proccess.start()

        while not self.exit:
            try: 
                socket = self._socket.accept()[0]
                self.try_clean_processes()
                if len(self.clients_processes.keys()) < MAX_AMOUNT_OF_CLIENTS:
                    client_uuid = uuid.uuid4().hex
                    # logger.info(f"Voy a crear el hilo con id: {client_uuid} para socket: {socket}")
                    client_proccess = Process(target=self.handle_client, args=(socket,client_uuid))
                    client_proccess.start()
                    self.clients_processes[client_uuid] = client_proccess
                elif socket:
                    logger.info("Client rejected due to max amount of clients reached")
            except OSError as err:
                logger.info(f"Error in socket: {err}")

        for id_, client_process in self.dict_procesos.items():
            client_process.join()
        results_proccess.join()

    def run(self):
        self.event = Event()
        cleaner_proccess = Process(target=self.run_cleaner, args=(self.event,))
        cleaner_proccess.start()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while not self.exit and cleaner_proccess.is_alive():
            msg = NODE_TYPE + "." + self.id + "$"
            try:
                for id,address in MEDIC_IP_ADDRESSES.items():
                    complete_addres = (address, MEDIC_PORT)
                    sock.sendto(msg.encode(), complete_addres)
                    logger.info(f"Heartbeat sent to medic with id: {id}")
            except Exception as e:
                logger.error(f"Error sending heartbeat: {e}")
            finally:
                time.sleep(HEARTBEAT_INTERVAL)
        cleaner_proccess.join()

def save_id(received_ids: dict, data_fragment: DataFragment) -> bool:
        client_id = data_fragment.get_client_id()
        query_id = data_fragment.get_query_id()
        id = data_fragment.get_id()
        received_ids[client_id] = received_ids.get(client_id, {})
        received_ids[client_id][query_id] = received_ids[client_id].get(query_id, set())
        if id in received_ids[client_id][query_id]:
            return False
        received_ids[client_id][query_id].add(id)
        return True

def main():
    cleaner = DataCleaner()
    cleaner.run()
    if not cleaner.exit:
        if cleaner.mom:
            cleaner.mom.close()
        if cleaner.log_writer:
            cleaner.log_writer.close()
   
if __name__ == "__main__":
    main()