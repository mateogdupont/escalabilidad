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
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.settimeout(TIMEOUT_LISTENER_SOCKET)
        self.id = int(os.environ.get("ID"))
        self.selected_as_lider_event = False
        self._ip = MEDIC_IPS[self.id]
        self._port = int(os.environ["PORT"])
        logger.info(f"Datos: {self._ip}")
        logger.info(f"Datos: {self._port}")
        self._socket.bind((self._ip, self._port))
        self._socket.listen(LISTEN_BACKLOG)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self._ip, self._port)) 
        self.sock.settimeout(SOCKET_TIMEOUT)
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

    def update_timeouts(self, node_id):
        self.nodes[node_id] = time.time()
        logger.info(f"Actulice el timeout para {node_id}")

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
                    container = self.create_container_name(node_type,node_id)
                    subprocess.run(["./medic/lunch_node.sh", container])
                else:
                    logger.info(f"should revive: {node_type}, with id  : {node_id} but im not the lider") #TODO: delete this
                keys_to_delete.append(key)
        for key in keys_to_delete:
            del self.nodes[key]

    def start_watchdog(self):
        address, port = self.sock.getsockname()
        while not self._finish_event.is_set():
            try:
                logger.error(f"Me voy a quedar colgado esperando el hatbeat")
                data, addr = self.sock.recvfrom(1024)
                # 1024 deberia ser suficiente para el hartbeat
                if not data:
                    continue
                msg = data.decode("utf-8")
                logger.info(f"Received: {msg}")
                if not '$'in msg or not '.'in msg:
                    logger.error(f"The msg does not respect the format: {msg}")
                self.verify_timeouts()
                self.update_timeouts(msg.replace("$",""))
            except socket.timeout:
                self.verify_timeouts()

    def send_election():
        #TODO: La idea aca es enviar a los que son mas altos que vos
        pass
   
    def try_update_sockets(self, peer_sockets, socket_queue):
        while True:
            # logger.error(f"Intento un update de los sockets")
            try:
                # Read [id,socket]
                msg = socket_queue.get_nowait()
                logger.info(f"Encontre al peer: {msg}")
                peer_sockets[msg[0]] = msg[1]
            except Exception:
                break
        return peer_sockets
    
    def send_bully_msg(self,peer_sockets,socket_queue_from_bully,msg_type, peers_to_send):
        logger.info(f"Voy a ver si hay algo en el sockets para enviar coordinated{peer_sockets}")
        msg = f"{self.id},{msg_type}"
        amount_of_msgs_send = 0
        new_sockets = {}
        for id in peers_to_send:
            if not id in peer_sockets.keys():
                logger.info(f"Sending {msg_type} and no socket for: {id}")
                medic_address = (MEDIC_IPS[id], self._port)
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.settimeout(TIMEOUT_LISTENER_SOCKET)
                logger.info(f"Me quiero conectar a: {MEDIC_IPS[id]} con puerto {self._port} e id {id}")
                try:
                    peer_socket.connect(medic_address)
                    peer_socket.settimeout(None)
                    sent_amount = send_msg(peer_socket,msg)
                    if sent_amount == 0:
                        logger.info(f"Error al intentar enviar a {peer_socket}")
                    else:
                        amount_of_msgs_send += 1
                        new_sockets[id] = peer_socket
                        socket_queue_from_bully.put([id,peer_socket])
                        logger.info(f"Me conecte a: {MEDIC_IPS[id]} con puerto {self._port} e id {id}")
                except Exception as e:
                    logger.info(f"Error in reconection: {e}")
                    continue
            else:
                sent_amount = send_msg(peer_sockets[id],msg)
                if sent_amount == 0:
                    logger.info(f"Error al intentar enviar a {peer_sockets[id]}")
                else:
                    amount_of_msgs_send += 1
        for new_id, new_socket in new_sockets.items():
            peer_sockets[new_id] = new_socket
        return amount_of_msgs_send
    
    def delete_peer_socket(self, peer_sockets, msg):
        logger.info(f"Voy a eliminar del dic y hay {peer_sockets}")
        ip_to_delete = msg.split(',')[2]
        id_to_delete = None
        for id,peer_socket in peer_sockets.items():
            try:
                peer_ip, peer_port = peer_socket.getpeername()
                if peer_ip == ip_to_delete:
                    id_to_delete = id
                    peer_socket.close()
            except:
                id_to_delete = id
                peer_socket.close()
        del peer_sockets[id_to_delete]
        logger.info(f"Elimine a la ip de {id_to_delete} y los sockets quedan {peer_sockets}")

    def revive_bigger_medics(self):
        for node_id in range(self.id + 1, MAX_MEDIC_ID + 1):
            container = self.create_container_name(NODE_TYPE,node_id)
            subprocess.run(["./medic/lunch_node.sh", container])

    def start_bully_administrator(self, socket_queue,socket_queue_from_bully, incoming_messages_queue):
        start_election_time = None
        start_coordination_time = None
        amount_of_coordinated_send = 0
        time_sinse_last_alive = None
        lider_id = None
        msg = None
        peer_sockets = {}
        logger.error(f"Inicio de bully")
        if self.id == MAX_MEDIC_ID:
            amount_of_coordinated_send = self.send_bully_msg(peer_sockets,socket_queue_from_bully,COORDINATOR_TYPE,range(1, self.id))
            start_coordination_time = time.time()
        logger.error(f"Entrando al super while del bully")
        while not self._finish_event.is_set():
            try:
                try:
                    msg = incoming_messages_queue.get(timeout=TIMEOUT_INCOMING_MSG)
                except Empty:
                    logger.info(f"Timeout in incoming messages y el timeout del election es: {start_election_time}")
                    peer_sockets = self.try_update_sockets(peer_sockets, socket_queue)

                    if time_sinse_last_alive and (time.time() - time_sinse_last_alive > SEND_ALIVE_TIMEOUT):
                        alive_msg = f"{self.id},{ALIVE_TYPE}"
                        try:
                            sent_amount = send_msg(peer_sockets[lider_id],alive_msg)
                            if sent_amount == 0:
                                raise ConnectionError
                            time_sinse_last_alive = time.time()
                            logger.info(f"El lider sigue vivo :)")
                        except Exception as e:
                            logger.info(f"Fallo de comunicacion con el lider por: {e}")
                            time_sinse_last_alive = None
                            self.send_bully_msg(peer_sockets,socket_queue_from_bully,ELECTION_TYPE,range(self.id + 1, MAX_MEDIC_ID + 1))
                            start_election_time = time.time()

                    if start_coordination_time and (time.time() - start_coordination_time > COORDINATOR_TIMEOUT):
                        logger.info(f"Me setee como lider por timeout")
                        lider_id = self.id
                        start_coordination_time = None
                        self.selected_as_lider_event.set()
                        self.revive_bigger_medics()

                    if start_election_time and (time.time() - start_election_time > ELECTION_TIMEOUT):
                        amount_of_coordinated_send = self.send_bully_msg(peer_sockets,socket_queue_from_bully,COORDINATOR_TYPE,range(1, self.id))
                        start_election_time = None
                        start_coordination_time = time.time()
                    
                finally:
                    if not msg:
                        continue
                    peer_sockets = self.try_update_sockets(peer_sockets, socket_queue)
                    msg_id = int(msg.split(',')[0])
                    msg_type = msg.split(',')[1]

                    logger.info(f"Me llego un mensaje por la queue: {msg}")
                    if msg_type == ELECTION_TYPE:
                        answer_msg= f"{self.id},{ANSWER_TYPE}"
                        self.send_bully_msg(peer_sockets,socket_queue_from_bully,ELECTION_TYPE,range(self.id + 1, MAX_MEDIC_ID + 1))
                        send_msg(peer_sockets[msg_id], answer_msg)
                        start_election_time = time.time()
                    elif msg_type == COORDINATOR_TYPE:
                        logger.info(f"Reconoci el coordinator")
                        lider_id = msg_id
                        time_sinse_last_alive = time.time()
                        start_election_time = None
                        ack_msg= f"{self.id},{ACK_TYPE}"
                        logger.info(f"El mensaje que voy a mandar es {ack_msg}")
                        self.selected_as_lider_event.clear()
                        logger.info(f"Limpio el lider event y tengo los peers: {peer_sockets} y msg_id {msg_id}")
                        if peer_sockets[msg_id]:
                            send_msg(peer_sockets[msg_id],ack_msg)
                            logger.info(f"Mande un ack del coordinator {msg}")
                    elif msg_type == ANSWER_TYPE:
                        start_election_time = None
                        self.selected_as_lider_event.clear()
                    elif msg_type == ALIVE_TYPE:
                        logger.info(f"El proceso {msg_id} me envio un alive")
                    elif msg_type == ACK_TYPE:
                        amount_of_coordinated_send -= 1
                        if amount_of_coordinated_send == 0:
                            logger.info(f"Me setee como lider por cantidad de ack")
                            start_coordination_time = None
                            lider_id = self.id
                            self.selected_as_lider_event.set()
                            self.revive_bigger_medics()
                    elif msg_type == DEAD_TYPE:
                        self.delete_peer_socket(peer_sockets, msg)
                    msg = None
            except Exception as e:
                #TODO
                logger.error(f"Fail with error: {e}")

            
    def start_msg_process(self, peer_socket, incoming_messages_queue: Queue):
        peer_ip, peer_port = peer_socket.getpeername()
        while not self._finish_event.is_set():
            try:
                msg = receive_msg(peer_socket)
                if not msg:
                    logger.error(f"MSG PROCESS:Fail to read msg, connection lost")
                    msg = f"{self.id},{DEAD_TYPE},{peer_ip}"
                    peer_socket.close()
                    incoming_messages_queue.put(msg)
                    return
                logger.info(f"MSG PROCESS: Me llego el mensaje: {msg}")
                incoming_messages_queue.put(msg)
            except Exception as e:
                peer_socket.close()
                msg = f"{self.id},{DEAD_TYPE},{peer_ip}"
                incoming_messages_queue.put(msg)
                logger.error(f"MSG PROCESS: Fail to read msg with error: {e}")
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
                peer_socket = self._socket.accept()[0]
                peer_id= self.get_id_from_address(peer_socket)
                socket_queue_from_listener.put([peer_id,peer_socket])
                logger.info(f"RECEIVER: lo mande por la queue")
                peer_sockets[peer_id] = peer_socket
            except Exception as e:
                # Handle timeout from listener
                try:
                    peer_sockets = self.try_update_sockets(peer_sockets, socket_queue_from_bully)
                except Exception as e:
                    logger.info(f"Error trying to update_sockets: {e}")
            finally:
                for id in peer_sockets:
                    logger.info(f"RECEIVER: Finally con {peer_sockets}")
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
    #graceful finish?
   
if __name__ == "__main__":
    main()