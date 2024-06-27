import subprocess
import sys
import os
import socket
import signal
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
SOCKET_TIMEOUT=10
LISTEN_BACKLOG = 10
TIMEOUT_LISTENER_SOCKET=0.4
TIMEOUT_INCOMING_MSG= 1
TIMEOUT_ALIVE_MEDIC=20

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
        while not self._finish_event.is_set():
            try:
                # Read [id,socket]
                msg = socket_queue.get_nowait()
                logger.debug(f"Bully| Read peer from socket_queue: {msg}")
                self.peer_sockets[int(msg[0])] = msg[1]
            except Exception:
                logger.debug(f"Bully| No new sockets in socket_queue")
                break
    
    def send_bully_msg(self,socket_queue_from_bully,msg_type, peers_to_send):
        msg = f"{self.id},{msg_type}"
        amount_of_msgs_send = 0
        new_sockets = {}
        for id in peers_to_send:
            if self._finish_event.is_set():
                break
            if not id in self.peer_sockets.keys():
                logger.info(f"Bully| Sending {msg_type} and no socket for: {id}")
                medic_address = (MEDIC_IPS[id], self._port)
                peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                peer_socket.settimeout(TIMEOUT_LISTENER_SOCKET)
                logger.info(f"Bully| Try connection with medic with id: {id}")
                try:
                    peer_socket.connect(medic_address)
                    peer_socket.settimeout(None)
                    sent_amount = send_msg(peer_socket,msg)
                    if sent_amount == 0:
                        logger.error(f"Bully| Error sending to socket: {peer_socket}")
                    else:
                        amount_of_msgs_send += 1
                        new_sockets[id] = peer_socket
                        socket_queue_from_bully.put([id,peer_socket])
                        logger.info(f"Bully| New connection with medic with medic with id: {id}")
                except Exception as e:
                    logger.error(f"Bully| Error in reconection: {e}")
                    continue
            else:
                sent_amount = send_msg(self.peer_sockets[id],msg)
                if sent_amount == 0:
                    logger.error(f"Bully| Error sending to socket: {self.peer_sockets[id]}")
                else:
                    amount_of_msgs_send += 1
        for new_id, new_socket in new_sockets.items():
            self.peer_sockets[new_id] = new_socket
        return amount_of_msgs_send
    
    def delete_peer_socket(self, peer_sockets, msg):
        logger.debug(f"Bully| Recived a socker to delete from: {peer_sockets} with msg: {msg}")
        ip_to_delete = msg.split(',')[2]
        id_to_delete = None
        for id,peer_socket in peer_sockets.items():
            if self._finish_event.is_set():
                break
            try:
                peer_ip, peer_port = peer_socket.getpeername()
                if peer_ip == ip_to_delete:
                    id_to_delete = id
                    peer_socket.close()
            except:
                id_to_delete = id
                peer_socket.close()
        del peer_sockets[id_to_delete]
    
    def create_container_name(self,node_type, node_id):
        resultado = CONTAINERS[node_type]+str(node_id)
        return resultado

    def revive_nodes(self, node_type, node_id):
        container = self.create_container_name(node_type,node_id)
        logger.info(f"Bully| Reviving container: {container}")
        subprocess.run([SCRIPT_PATH, container])

    def revive_bigger_medics(self):
        if self.id == MAX_MEDIC_ID:
            return
        for node_id in range(self.id + 1, MAX_MEDIC_ID + 1):
            self.revive_nodes(NODE_TYPE,node_id)

    def handle_election_msg(self, socket_queue_from_bully, msg_id):
        logger.info(f"Bully| Processing msg of type election")
        answer_msg= f"{self.id},{ANSWER_TYPE}"
        self.send_bully_msg(socket_queue_from_bully,ELECTION_TYPE,range(self.id + 1, MAX_MEDIC_ID + 1))
        send_msg(self.peer_sockets[msg_id], answer_msg)
        self.start_election_time = time.time()
        self.time_verify_non_lider_medics = None
        logger.info(f"Bully| Replied with an ack and sent election_msg to bigger id medics")
    
    def handle_coordinator_msg(self, msg_id):
        logger.info(f"Bully| Processing msg of type coordinator")
        self.lider_id = msg_id
        self.time_last_alive_sent = time.time()
        self.start_election_time = None
        ack_msg= f"{self.id},{ACK_TYPE}"
        self.selected_as_lider_event.clear()
        if self.peer_sockets[msg_id]:
            send_msg(self.peer_sockets[msg_id],ack_msg)
            logger.info(f"Bully|Sent ack_msg for coordinator")

    def handle_ack_msg(self):
        logger.info(f"Bully| Processing msg of type ack")
        self.amount_of_coordinated_send -= 1
        if self.amount_of_coordinated_send == 0:
            logger.info(f"Bully| Set myself ({self.id}) as lider by amount of ack")
            self.start_coordination_time = None
            self.lider_id = self.id
            self.selected_as_lider_event.set()
            self.revive_bigger_medics()
            if self.id == MAX_MEDIC_ID:
                self.time_verify_non_lider_medics = time.time()

    def process_bully_msg(self,socket_queue_from_bully, socket_queue, msg):
        logger.info(f"Bully| Read msg from incoming_messages_queue: {msg}")
        self.try_update_sockets(socket_queue)
        msg_id = int(msg.split(',')[0])
        msg_type = int(msg.split(',')[1])
        self.alive_timeouts.add(msg_id)

        if msg_type == ELECTION_TYPE:
            self.handle_election_msg(socket_queue_from_bully, msg_id)
        elif msg_type == COORDINATOR_TYPE:
            self.handle_coordinator_msg(msg_id)
        elif msg_type == ANSWER_TYPE:
            self.start_election_time = None
            self.selected_as_lider_event.clear()
        elif msg_type == ALIVE_TYPE:
            logger.info(f"Bully| {msg_id} sent an alive msg")
        elif msg_type == ACK_TYPE:
            self.handle_ack_msg()
        elif msg_type == DEAD_TYPE:
            self.delete_peer_socket(self.peer_sockets, msg)

    def send_alive_msg(self, socket_queue_from_bully):
        alive_msg = f"{self.id},{ALIVE_TYPE}"
        try:
            sent_amount = send_msg(self.peer_sockets[self.lider_id],alive_msg)
            if sent_amount == 0:
                raise ConnectionError
            self.time_last_alive_sent = time.time()
            logger.debug(f"Bully| Sent alive msg to lider successfully")
        except Exception as e:
            logger.error(f"Bully| Fail in communication with lider with error: {e}")
            self.time_last_alive_sent = None
            self.send_bully_msg(socket_queue_from_bully,ELECTION_TYPE,range(self.id + 1, MAX_MEDIC_ID + 1))
            self.start_election_time = time.time()

    def start_bully_by_id(self, socket_queue_from_bully):
        if self.id == MAX_MEDIC_ID:
            self.amount_of_coordinated_send = self.send_bully_msg(socket_queue_from_bully,COORDINATOR_TYPE,range(1, self.id))
            self.start_coordination_time = time.time()
        else:
            self.send_alive_msg(socket_queue_from_bully)
            
    
    def verify_non_lider_medics_timeout(self):
        if self.time_verify_non_lider_medics and (time.time() - self.time_verify_non_lider_medics > (TIMEOUT_ALIVE_MEDIC)):
            for node_id in range(1, MAX_MEDIC_ID):
                if not node_id in self.alive_timeouts:
                    self.revive_nodes(NODE_TYPE,node_id)
            self.alive_timeouts = set()
            self.time_verify_non_lider_medics = time.time()
    
    def verify_sent_alive_timeout(self, socket_queue_from_bully):
        if self.id == self.lider_id:
            return
        if self.time_last_alive_sent and (time.time() - self.time_last_alive_sent > SEND_ALIVE_TIMEOUT):
            self.send_alive_msg(socket_queue_from_bully)

    def verify_coordination_timeout(self):
        if self.start_coordination_time and (time.time() - self.start_coordination_time > COORDINATOR_TIMEOUT):
            logger.info(f"Bully| Set myself ({self.id}) as lider by timeout")
            self.lider_id = self.id
            self.start_coordination_time = None
            if self.id == MAX_MEDIC_ID:
                self.time_verify_non_lider_medics = time.time()
            self.selected_as_lider_event.set()
            self.revive_bigger_medics()

    def verify_election_timeout(self, socket_queue_from_bully):
        if self.start_election_time and (time.time() - self.start_election_time > ELECTION_TIMEOUT):
            logger.info(f"Bully| Detected_election_msg sent timeout")      
            self.amount_of_coordinated_send = self.send_bully_msg(socket_queue_from_bully,COORDINATOR_TYPE,range(1, self.id))
            self.start_election_time = None
            self.start_coordination_time = time.time()
            self.time_verify_non_lider_medics = None

    def start(self, socket_queue_from_bully: Queue, incoming_messages_queue: Queue,socket_queue: Queue):
        logger.info(f"Bully| Starting bully in node: {self.id}")
        msg = None
        self.start_bully_by_id(socket_queue_from_bully)

        while not self._finish_event.is_set():
            try:
                try:
                    self.try_update_sockets(socket_queue)
                    self.verify_non_lider_medics_timeout()
                    self.verify_sent_alive_timeout(socket_queue_from_bully)
                    self.verify_coordination_timeout()
                    self.verify_election_timeout(socket_queue_from_bully)
                    msg = incoming_messages_queue.get(timeout=TIMEOUT_INCOMING_MSG)
                except Empty:
                    logger.info(f"Bully| Incoming_messages is Empty")       
                finally:
                    if not msg:
                        continue
                    self.try_update_sockets(socket_queue)
                    self.process_bully_msg(socket_queue_from_bully, socket_queue, msg)
                    msg = None
            except Exception as e:
                logger.error(f"Bully| Fail with error: {e}")

        for medic_socket in self.peer_sockets:
            medic_socket.close()