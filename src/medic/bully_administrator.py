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

MAX_MEDIC_ID=4
NODE_TYPE=7

SCRIPT_PATH = "./medic/lunch_node.sh"

CONTAINERS={1:"cleaner", 2:"filter",3:"joiner",4:"counter",5:"sentiment-analyzer",6:"rabbitmq",7:"medic"}
MEDIC_IPS={1:"172.16.238.10",2:"172.16.238.20",3:"172.16.238.30",4:"172.16.238.40"}

COORDINATOR_TYPE=1
ELECTION_TYPE=2
ALIVE_TYPE=3
ANSWER_TYPE=4
ACK_TYPE=5
DEAD_TYPE=6

COORDINATOR_TIMEOUT=4
SEND_ALIVE_TIMEOUT=1
ELECTION_TIMEOUT=8
SOCKET_TIMEOUT=101
LISTEN_BACKLOG = 4
TIMEOUT_LISTENER_SOCKET=0.4
TIMEOUT_INCOMING_MSG= 1

class Bully:
    def __init__(self, node_id, port,selected_as_lider_event, finish_event):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        self.id= node_id
        self._port= port
        self.start_election_time = None
        self.start_coordination_time = None
        self.time_last_alive_sent = None
        self.time_verify_non_lider_medics=None
        self.alive_timeouts = set()
        self.amount_of_coordinated_send = 0
        self.lider_id = MAX_MEDIC_ID
        self.peer_sockets = {}
        self.selected_as_lider_event= selected_as_lider_event
        self._finish_event = finish_event


    def try_update_sockets(self, socket_queue):
        while True:
            try:
                # Read [id,socket]
                msg = socket_queue.get_nowait()
                logger.info(f"Encontre al peer: {msg}")
                self.peer_sockets[int(msg[0])] = msg[1]
                logger.info(f"Lo pude guardar bien")
            except Exception as e:
                logger.info(f"No pude update por: {e}")
                break
    
    def send_bully_msg(self,socket_queue_from_bully,msg_type, peers_to_send):
        logger.info(f"Voy a ver si hay algo en el sockets para enviar coordinated{self.peer_sockets}")
        msg = f"{self.id},{msg_type}"
        amount_of_msgs_send = 0
        new_sockets = {}
        for id in peers_to_send:
            if not id in self.peer_sockets.keys():
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
                sent_amount = send_msg(self.peer_sockets[id],msg)
                if sent_amount == 0:
                    logger.info(f"Error al intentar enviar a {self.peer_sockets[id]}")
                else:
                    amount_of_msgs_send += 1
        for new_id, new_socket in new_sockets.items():
            self.peer_sockets[new_id] = new_socket
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
    
    def create_container_name(self,node_type, node_id):
        resultado = CONTAINERS[node_type]+str(node_id)
        return resultado

    def revive_nodes(self, node_type, node_id):
        container = self.create_container_name(node_type,node_id)
        subprocess.run([SCRIPT_PATH, container])

    def revive_bigger_medics(self):
        if self.id == MAX_MEDIC_ID:
            return
        for node_id in range(self.id + 1, MAX_MEDIC_ID + 1):
            self.revive_nodes(NODE_TYPE,node_id)

    def handle_election_msg(self, socket_queue_from_bully, msg_id):
        logger.info(f"Me llego un ELECTION")
        answer_msg= f"{self.id},{ANSWER_TYPE}"
        logger.info(f"Le voy a responder: {answer_msg}")
        self.send_bully_msg(socket_queue_from_bully,ELECTION_TYPE,range(self.id + 1, MAX_MEDIC_ID + 1))
        logger.info(f"Mande el ELECTION a superiores y mando answer a {self.peer_sockets[msg_id]}")
        send_msg(self.peer_sockets[msg_id], answer_msg)
        logger.info(f"Mande el answer")
        self.start_election_time = time.time()
        self.time_verify_non_lider_medics = None
        logger.info(f"Seteo time en {self.start_election_time}")

    def process_bully_msg(self,socket_queue_from_bully, socket_queue, msg):
        logger.info(f"Me llego un mensaje por la queue: {msg}")
        self.try_update_sockets(socket_queue)
        msg_id = int(msg.split(',')[0])
        msg_type = int(msg.split(',')[1])
        self.alive_timeouts.add(msg_id)

        if msg_type == ELECTION_TYPE:
            self.handle_election_msg(socket_queue_from_bully, msg_id)
        elif msg_type == COORDINATOR_TYPE:
            logger.info(f"Reconoci el coordinator")
            self.lider_id = msg_id
            self.time_last_alive_sent = time.time()
            self.start_election_time = None
            ack_msg= f"{self.id},{ACK_TYPE}"
            logger.info(f"El mensaje que voy a mandar es {ack_msg}")
            self.selected_as_lider_event.clear()
            logger.info(f"Limpio el lider event y tengo los peers: {self.peer_sockets} y msg_id {msg_id}")
            if self.peer_sockets[msg_id]:
                send_msg(self.peer_sockets[msg_id],ack_msg)
                logger.info(f"Mande un ack del coordinator {msg}")
        elif msg_type == ANSWER_TYPE:
            self.start_election_time = None
            self.selected_as_lider_event.clear()
        elif msg_type == ALIVE_TYPE:
            logger.info(f"El proceso {msg_id} me envio un alive")
        elif msg_type == ACK_TYPE:
            self.amount_of_coordinated_send -= 1
            if self.amount_of_coordinated_send == 0:
                logger.info(f"Me setee como lider por cantidad de ack")
                self.start_coordination_time = None
                self.lider_id = self.id
                self.selected_as_lider_event.set()
                self.revive_bigger_medics()
                if self.id == MAX_MEDIC_ID:
                    self.time_verify_non_lider_medics = time.time()
        elif msg_type == DEAD_TYPE:
            self.delete_peer_socket(self.peer_sockets, msg)

    def start(self, socket_queue_from_bully: Queue, incoming_messages_queue: Queue,socket_queue: Queue):
        logger.info(f"Inicio de bully")
        msg = None
        if self.id == MAX_MEDIC_ID:
            self.amount_of_coordinated_send = self.send_bully_msg(socket_queue_from_bully,COORDINATOR_TYPE,range(1, self.id))
            self.start_coordination_time = time.time()
        else:
            logger.info(f"Entre al else")
            alive_msg = f"{self.id},{ALIVE_TYPE}"
            try:
                logger.info(f"Antes de mandar")
                sent_amount = send_msg(self.peer_sockets[self.lider_id],alive_msg)
                logger.info(f"Desp de mandar")
                if sent_amount == 0:
                    raise ConnectionError
                self.time_last_alive_sent = time.time()
                logger.info(f"El lider sigue vivo :)")
            except Exception as e:
                logger.info(f"Fallo de comunicacion con el lider por: {e}")
                self.time_last_alive_sent = None
                self.send_bully_msg(socket_queue_from_bully,ELECTION_TYPE,range(self.id + 1, MAX_MEDIC_ID + 1))
                self.start_election_time = time.time()

        logger.error(f"Entrando al super while del bully")
        while not self._finish_event.is_set():
            try:
                try:
                    msg = incoming_messages_queue.get(timeout=TIMEOUT_INCOMING_MSG)
                except Empty:
                    logger.info(f"Timeout in incoming messages y el timeout del election es: {self.start_election_time}")
                    logger.info(f"Los que estan en alive son: {self.alive_timeouts}")
                    if self.time_verify_non_lider_medics:
                        logger.info(f"El timeout de alive es: {self.time_verify_non_lider_medics} contra: {time.time() - self.time_verify_non_lider_medics} y {2*SEND_ALIVE_TIMEOUT}")
                    else:
                        logger.info(f"El timeout de alive es: None xd")
                    self.try_update_sockets(socket_queue)

                    if self.time_verify_non_lider_medics and (time.time() - self.time_verify_non_lider_medics > (2*SEND_ALIVE_TIMEOUT)):
                        for node_id in range(1, MAX_MEDIC_ID):
                            if not node_id in self.alive_timeouts:
                                self.revive_nodes(NODE_TYPE,node_id)
                        self.alive_timeouts = set()
                        self.time_verify_non_lider_medics = time.time()

                    if self.time_last_alive_sent and (time.time() - self.time_last_alive_sent > SEND_ALIVE_TIMEOUT):
                        alive_msg = f"{self.id},{ALIVE_TYPE}"
                        try:
                            sent_amount = send_msg(self.peer_sockets[self.lider_id],alive_msg)
                            if sent_amount == 0:
                                raise ConnectionError
                            self.time_last_alive_sent = time.time()
                            logger.info(f"El lider sigue vivo :)")
                        except Exception as e:
                            logger.info(f"Fallo de comunicacion con el lider por: {e}")
                            self.time_last_alive_sent = None
                            self.send_bully_msg(socket_queue_from_bully,ELECTION_TYPE,range(self.id + 1, MAX_MEDIC_ID + 1))
                            self.start_election_time = time.time()

                    if self.start_coordination_time and (time.time() - self.start_coordination_time > COORDINATOR_TIMEOUT):
                        logger.info(f"Me setee como lider por timeout")
                        self.lider_id = self.id
                        self.start_coordination_time = None
                        if self.id == MAX_MEDIC_ID:
                            self.time_verify_non_lider_medics = time.time()
                        self.selected_as_lider_event.set()
                        self.revive_bigger_medics()

                    if self.start_election_time and (time.time() - self.start_election_time > ELECTION_TIMEOUT):
                        self.amount_of_coordinated_send = self.send_bully_msg(socket_queue_from_bully,COORDINATOR_TYPE,range(1, self.id))
                        self.start_election_time = None
                        self.start_coordination_time = time.time()
                        self.time_verify_non_lider_medics = None
                    
                finally:
                    if not msg:
                        continue
                    self.process_bully_msg(socket_queue_from_bully, socket_queue, msg)
                    msg = None
            except Exception as e:
                #TODO
                logger.error(f"Fail with error: {e}")