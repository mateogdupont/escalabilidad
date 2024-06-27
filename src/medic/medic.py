import subprocess
import sys
import os
import socket
import signal
from dotenv import load_dotenv # type: ignore
import logging as logger
from multiprocessing import Process, Event, Queue
from queue import Empty
from utils.stream_communications import *
from bully_administrator import *
import time

load_dotenv()
TIMEOUT=float(os.environ.get("TIMEOUT"))
CONTAINERS=eval(os.environ.get("CONTAINERS"))
NODE_TYPE=int(os.environ.get("NODE_TYPE"))
MEDIC_IPS=eval(os.environ.get("MEDIC_IPS"))
SOCKET_TIMEOUT=int(os.environ.get("SOCKET_TIMEOUT"))
COORDINATOR_TIMEOUT=int(os.environ.get("COORDINATOR_TIMEOUT"))
SEND_ALIVE_TIMEOUT=int(os.environ.get("SEND_ALIVE_TIMEOUT"))
ELECTION_TIMEOUT=int(os.environ.get("ELECTION_TIMEOUT"))
MAX_MEDIC_ID=int(os.environ.get("MAX_MEDIC_ID"))
LISTEN_BACKLOG = 4
TIMEOUT_LISTENER_SOCKET=0.4
TIMEOUT_INCOMING_MSG= 1
COORDINATOR_TYPE=os.environ.get("COORDINATOR_TYPE")
ELECTION_TYPE=os.environ.get("ELECTION_TYPE")
COORDINATOR_TYPE=os.environ.get("COORDINATOR_TYPE")
ANSWER_TYPE=os.environ.get("ANSWER_TYPE")
ACK_TYPE=os.environ.get("ACK_TYPE")
ALIVE_TYPE=os.environ.get("ALIVE_TYPE")
DEAD_TYPE=os.environ.get("DEAD_TYPE")


class Medic:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        self._tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._tcp_socket.settimeout(TIMEOUT_LISTENER_SOCKET)
        self.id = int(os.environ.get("ID"))
        self.selected_as_lider_event = False
        self._ip = MEDIC_IPS[self.id]
        self._port = int(os.environ["PORT"])
        self._udp_port = int(os.environ["UDP_PORT"])
        self._tcp_socket.bind((self._ip, self._port))
        self._tcp_socket.listen(LISTEN_BACKLOG)
        self._udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udp_socket.bind((self._ip, self._udp_port))
        self._udp_socket.settimeout(SOCKET_TIMEOUT)
        self._stop = False
        self._finish_event = None
        self.nodes = {}

        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)

    def sigterm_handler(self, signal,frame):
        self._stop = True
        if self._finish_event:
            self._finish_event.set()

    def parse_id(self, complete_id):
        parts = complete_id.split(".")
        return tuple(map(int, parts))
    
    def revive_nodes(self, node_type, node_id):
            container = self.create_container_name(node_type,node_id)
            subprocess.run(["./medic/lunch_node.sh", container])

    def update_timeouts(self, node_id):
        self.nodes[node_id] = time.time()

    def create_container_name(self,node_type, node_id):
        resultado = CONTAINERS[node_type]+str(node_id)
        return resultado
    
    def verify_timeouts(self):
        keys_to_delete=[]
        for key, value in self.nodes.items():
            if time.time() - value > TIMEOUT:
                (node_type, node_id) = self.parse_id(key)
                if self.selected_as_lider_event.is_set():
                    logger.info(f"A node has died, reviving type: {node_type}, with id  : {node_id}")
                    self.revive_nodes(node_type,node_id)
                else:
                    logger.info(f"Should revive: {node_type}, with id  : {node_id} but im not the lider") #TODO: delete this
                keys_to_delete.append(key)
        for key in keys_to_delete:
            del self.nodes[key]

    def start_watchdog(self):
        address, port = self._udp_socket.getsockname()
        while not self._finish_event.is_set():
            try:
                data, addr = self._udp_socket.recvfrom(1024)
                # 1024 deberia ser suficiente para el hartbeat
                if not data:
                    continue
                msg = data.decode("utf-8")
                # logger.info(f"Received: {msg}")
                if not '$'in msg or not '.'in msg:
                    logger.error(f"WatchDog| The msg does not respect the format: {msg}")
                self.verify_timeouts()
                self.update_timeouts(msg.replace("$",""))
            except socket.timeout:
                self.verify_timeouts()
   
    def try_update_sockets(self, peer_sockets, socket_queue):
        while True:
            try:
                # Read [id,socket]
                msg = socket_queue.get_nowait()
                peer_sockets[msg[0]] = msg[1]
            except Exception:
                break
        return peer_sockets

    def start_bully_administrator(self, socket_queue,socket_queue_from_bully, incoming_messages_queue):
        time.sleep(TIMEOUT) #Sleep to have the same information as the rest of medics
        bully = Bully(self.id,self._port,self.selected_as_lider_event,self._finish_event)
        bully.start(socket_queue_from_bully,incoming_messages_queue,socket_queue)
            
    def start_msg_process(self, peer_socket, incoming_messages_queue: Queue):
        peer_ip, peer_port = peer_socket.getpeername()
        while not self._finish_event.is_set():
            try:
                msg = receive_msg(peer_socket)
                if not msg:
                    logger.error(f"MSG PROCESS| Fail to read msg, connection lost")
                    msg = f"{self.id},{DEAD_TYPE},{peer_ip}"
                    peer_socket.close()
                    incoming_messages_queue.put(msg)
                    return
                logger.info(f"MSG PROCESS| Read msg: {msg}")
                incoming_messages_queue.put(msg)
            except Exception as e:
                peer_socket.close()
                msg = f"{self.id},{DEAD_TYPE},{peer_ip}"
                incoming_messages_queue.put(msg)
                logger.error(f"MSG PROCESS| Fail to read msg with error: {e}")
                return

    def get_id_from_address(self, peer_socket):
        peer_ip, peer_port = peer_socket.getpeername()
        for id,ip in MEDIC_IPS.items():
            if ip == peer_ip:
                return id
        return None

    def run(self):
        self._finish_event = Event()
        self.selected_as_lider_event = Event()
        watchdog = Process(target=self.start_watchdog)
        watchdog.start()
    
        socket_queue_from_listener = Queue()
        socket_queue_from_bully = Queue()
        incoming_messages_queue = Queue()
        bully_administrator = Process(target=self.start_bully_administrator, args=(socket_queue_from_listener,socket_queue_from_bully, incoming_messages_queue))
        bully_administrator.start()

        msg_processes = []

        while not self._stop:
            peer_sockets = {}
            try:
                peer_socket = self._tcp_socket.accept()[0]
                peer_id= self.get_id_from_address(peer_socket)
                socket_queue_from_listener.put([peer_id,peer_socket])
                logger.info(f"RECEIVER| Put msg in queue")
                peer_sockets[peer_id] = peer_socket
            except Exception as e:
                # Handle timeout from listener
                try:
                    peer_sockets = self.try_update_sockets(peer_sockets, socket_queue_from_bully)
                except Exception as e:
                    logger.info(f"RECEIVER| Error trying to update_sockets: {e}")
            finally:
                for id in peer_sockets:
                    msg_process = Process(target=self.start_msg_process, args=(peer_sockets[id], incoming_messages_queue))
                    msg_process.start()
                    msg_processes.append(msg_process)
                peer_sockets = {}


        watchdog.join()
        bully_administrator.join()
        for process in msg_processes:
            process.join()

def main():
    load_dotenv()
    Medic().run()
   
if __name__ == "__main__":
    main()