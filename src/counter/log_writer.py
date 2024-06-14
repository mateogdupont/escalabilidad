from typing import Tuple
from utils.log_manager.basic_log_writer import *
from utils.structs.data_fragment import DataFragment
import pickle

RECEIVED_ID = "RECEIVED_ID"             # <client_id> <query_id> <df_id>
# COUNTED_DATA = "COUNTED_DATA"         # <client_id> <query_id> <self.counted_data[client_id][query_id] como str>
COUNTED_DATA = "COUNTED_DATA"           # <client_id> <query_id> <count info>
QUERY_ENDED = "QUERY_ENDED"             # <client_id> <query_id>
COUNTED_DATA_SENT = "COUNTED_DATA_SENT" # <client_id> <query_id>

class LogWriter(BasicLogWriter):
    def __init__(self, log_queue: str, routing_key: str) -> None:
        super().__init__(log_queue, routing_key)

    def log_counted_data(self, fragment: DataFragment, count_info: str) -> None:
        client_id = fragment.get_client_id()
        query_id = fragment.get_query_id()
        df_id = fragment.get_id()
        id_log = f"{RECEIVED_ID} {client_id} {query_id} {df_id}"
        counted_log = f"{COUNTED_DATA} {client_id} {query_id} {count_info}"
        self._add_logs([id_log, counted_log])

    def log_counted_data_sent(self, fragment: DataFragment) -> None:
        client_id = fragment.get_client_id()
        query_id = fragment.get_query_id()
        df_id = fragment.get_id()
        id_log = f"{RECEIVED_ID} {client_id} {query_id} {df_id}"
        sent_log = f"{COUNTED_DATA_SENT} {client_id} {query_id}"
        self._add_logs([id_log, sent_log])