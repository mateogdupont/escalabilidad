import sys
import os
import socket
from client import Client

PORT = 1250
CLEANER_IP = 'cleaner'
# App must be executed with the following format:
# python main.py PATH_OF_DATA_DIRECTORY QUERIES
# Example of use:
# python main.py ./data 1,2,3,4,5
def main():
    if len(sys.argv) != 3 or not os.path.exists(sys.argv[1]):
        print("Error: Arguments error")
        return
    queries = [int(num) for num in sys.argv[2].split(",")]
    data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cleaner_address = (CLEANER_IP, PORT)
    data_socket.connect(cleaner_address)
    client = Client(sys.argv[1], dict.fromkeys(queries, 0), data_socket)
    client.run()
   
if __name__ == "__main__":
    main()