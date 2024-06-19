from typing import Tuple
from utils.log_manager.basic_log_writer import *
from utils.structs.data_fragment import DataFragment
import pickle

NEW_CLIENT = "NEW_CLIENT"
ENDED_CLIENT = "ENDED_CLIENT"

class LogWriter(BasicLogWriter):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
    
    def log_new_client(self, client_id: str) -> None:
        log = f"{NEW_CLIENT} {client_id}"
        self._add_logs([log])
    
    def log_ended_client(self, client_id: str) -> None:
        log = f"{ENDED_CLIENT} {client_id}"
        self._add_logs([log])
    