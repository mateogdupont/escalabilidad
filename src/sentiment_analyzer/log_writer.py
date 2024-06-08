from typing import Tuple
from utils.log_manager.basic_log_writer import *
from utils.structs.data_fragment import DataFragment

RECEIVED_ID = "RECEIVED_ID" # <client_id> <query_id> <df_id>
RESULT = "RESULT"           # <node> <datafragment como str>
QUERY_ENDED = "QUERY_ENDED" # <client_id> <query_id>
RESULT_SENT = "RESULT_SENT" # <node>

class LogWriter(BasicLogWriter):
    def __init__(self, log_queue: str, routing_key: str) -> None:
        super().__init__(log_queue, routing_key)
