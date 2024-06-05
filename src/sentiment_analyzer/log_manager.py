from typing import Tuple
from utils.basic_log_manager import *
from utils.structs.data_fragment import DataFragment

RECEIVED_ID = "RECEIVED_ID" # <client_id> <query_id> <df_id>
RESULT = "RESULT"           # <node> <datafragment como str>
QUERY_ENDED = "QUERY_ENDED" # <client_id> <query_id>
RESULT_SENT = "RESULT_SENT" # <node>

class LogManager(BasicLogManager):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
