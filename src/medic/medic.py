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
CONTAINERS=os.environ.get("CONTAINERS")

class Medic:
    def __init__(self):
        logger.basicConfig(stream=sys.stdout, level=logger.INFO)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(5)
        self._ip = os.environ["MEDIC_IP"]
        self._port = os.environ["PORT"]
        self._stop = False

        creado = time.time()
        self.nodes = {"1.2": creado}

        signal.signal(signal.SIGTERM, self.sigterm_handler)
        signal.signal(signal.SIGINT, self.sigterm_handler)

    def sigterm_handler(self, signal,frame):
        self._stop = True

    def parse_id(self, complete_id):
        parts = complete_id.split(".")
        return tuple(map(int, parts))

    def update_timeouts(self, node_id):
        self.nodes[node_id] = time.time()

    def create_container_name(self,node_type, node_id):
        return CONTAINERS[node_type]+str(node_id)

    #msg: 1.1$
    def run(self):
        while not self._stop:
            try:
                logger.info(f"Me voy a poner a escuchar")
                data, addr = self.sock.recvfrom(1024)
                # 1024 deberia ser suficiente para el hartbeat
                if not data:
                    continue
                msg = data.decode("utf-8")
                if not '$'in msg or not '.'in msg:
                    logger.error(f"The msg does not respect the format: {msg}")

                self.update_timeouts(msg.replace("$",""))
            except socket.timeout:
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
                

def main():
    load_dotenv()
    Medic().run()
    #graceful finish?
   
if __name__ == "__main__":
    main()