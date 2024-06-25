from typing import Tuple
from utils.log_manager.basic_log_recoverer import *
from utils.structs.data_fragment import DataFragment

NEW_CLIENT_PARTS = 2
ENDED_CLIENT_PARTS = 2

NEW_CLIENT = "NEW_CLIENT"
ENDED_CLIENT = "ENDED_CLIENT"

class LogRecoverer(BasicLogRecoverer):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self.ended_clients = set()
        self.clients = set()
        self._recover_funcs.update({
            NEW_CLIENT: self._process_new_client,
            ENDED_CLIENT: self._process_ended_client
        })

    def _process_new_client(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) < NEW_CLIENT_PARTS:
            return False
        _, client_id = parts
        if client_id in self.ended_clients:
            return False
        self.clients.add(client_id)
        return True
    
    def _process_ended_client(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) < ENDED_CLIENT_PARTS:
            return False
        _, client_id = parts
        self.ended_clients.add(client_id)
        return True

    def get_clients(self) -> set:
        return self.clients