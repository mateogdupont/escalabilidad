from typing import Tuple
from utils.log_manager.basic_log_writer import *
from utils.structs.data_fragment import DataFragment
import pickle

RECEIVED_ID = "RECEIVED_ID" # <client_id> <query_id> <df_id>
RESULT = "RESULT"           # <node> <time> <datafragment como str>
QUERY_ENDED = "QUERY_ENDED" # <client_id> <query_id>
RESULT_SENT = "RESULT_SENT" # <node>

class LogWriter(BasicLogWriter):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
    
    def log_received_id(self, fragment: DataFragment) -> None:
        client_id = fragment.get_client_id()
        query_id = fragment.get_query_id()
        df_id = fragment.get_id()
        id_log = f"{RECEIVED_ID} {client_id} {query_id} {df_id}"
        self._add_logs([id_log])

    