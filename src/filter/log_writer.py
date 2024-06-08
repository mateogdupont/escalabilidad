from typing import Tuple
from utils.log_manager.basic_log_writer import *
from utils.structs.data_fragment import DataFragment
import pickle

RECEIVED_ID = "RECEIVED_ID" # <client_id> <query_id> <df_id>
RESULT = "RESULT"           # <node> <time> <datafragment como str>
TOP_UPDATE = "TOP_UPDATE"   # <client_id> <query_id> <self.top_ten[client_id][query_id] como str>
QUERY_ENDED = "QUERY_ENDED" # <client_id> <query_id>
RESULT_SENT = "RESULT_SENT" # <node>

TOP_UPDATE_PRIORITY = LOW

class LogWriter(BasicLogWriter):
    def __init__(self, log_queue: str, routing_key: str) -> None:
        super().__init__(log_queue, routing_key)

    def log_top_update(self, fragment: DataFragment, top_ten: List[DataFragment]) -> None:
        client_id = fragment.get_client_id()
        query_id = fragment.get_query_id()
        df_id = fragment.get_id()
        id_log = f"{RECEIVED_ID} {client_id} {query_id} {df_id}"
        top_log = f"{TOP_UPDATE} {client_id} {query_id} {pickle.dumps(top_ten)}"
        self._add_logs({id_log: RECEIVED_ID_PRIORITY, top_log: TOP_UPDATE_PRIORITY})
    
    def log_received_id(self, fragment: DataFragment) -> None:
        client_id = fragment.get_client_id()
        query_id = fragment.get_query_id()
        df_id = fragment.get_id()
        id_log = f"{RECEIVED_ID} {client_id} {query_id} {df_id}"
        self._add_logs({id_log: RECEIVED_ID_PRIORITY})

    