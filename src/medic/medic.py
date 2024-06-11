import subprocess
import sys
import os
import socket
import signal
from dotenv import load_dotenv # type: ignore
import logging as logger
import time

load_dotenv()
TIMEOUT=float(os.environ.get("TIMEOUT"))
CONTAINERS=eval(os.environ.get("CONTAINERS"))
SOCKET_TIMEOUT=int(os.environ.get("SOCKET_TIMEOUT"))

class Medic:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        self._ip = os.environ["MEDIC_IP"]
        self._port = int(os.environ["PORT"])
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self._ip, self._port)) 
        self.sock.settimeout(SOCKET_TIMEOUT)
        self._stop = False
        self.nodes = {}

        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)

    def sigterm_handler(self, signal,frame):
        self._stop = True

    def parse_id(self, complete_id):
        parts = complete_id.split(".")
        return tuple(map(int, parts))

    def update_timeouts(self, node_id):
        self.nodes[node_id] = time.time()
        logger.info(f"Actulice el timeout para {node_id}")

    def create_container_name(self,node_type, node_id):
        print(F"el node_type es: {type(node_type)} y el otro {type(node_id)}")
        resultado = CONTAINERS[node_type]+str(node_id)
        print(f"El resultado del name es: {CONTAINERS[node_type]} + {str(node_id)} = {resultado}")
        return resultado
    
    def verify_timeouts(self):
        keys_to_delete=[]
        for key, value in self.nodes.items():
            if time.time() - value > TIMEOUT:
                (node_type, node_id) = self.parse_id(key)
                logger.info(f"Se murio, hay re revivir el nodo de tipo: {node_type} y id: {node_id}")
                container = self.create_container_name(node_type,node_id)
                subprocess.run(["./medic/lunch_node.sh", container])
                keys_to_delete.append(key)
        for key in keys_to_delete:
            del self.nodes[key]

    #msg: 1.1$
    def run(self):
        address, port = self.sock.getsockname()
        while not self._stop:
            try:
                logger.info(f"Me voy a poner a escuchar en {address}, {port}")
                data, addr = self.sock.recvfrom(1024)
                # 1024 deberia ser suficiente para el hartbeat
                if not data:
                    continue
                msg = data.decode("utf-8")
                logger.info(f"Recibi {msg}")
                if not '$'in msg or not '.'in msg:
                    logger.error(f"The msg does not respect the format: {msg}")
                self.verify_timeouts()
                self.update_timeouts(msg.replace("$",""))
            except socket.timeout:
                self.verify_timeouts()
                
                

def main():
    load_dotenv()
    Medic().run()
    #graceful finish?
   
if __name__ == "__main__":
    main()