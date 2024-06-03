from utils.basic_log_manager import *
from utils.structs.data_fragment import DataFragment

RECEIVED_ID = "RECEIVED_ID"
RESULT = "RESULT"
QUERY_ENDED = "QUERY_ENDED"
RESULT_SENT = "RESULT_SENT"

class LogManager(BasicLogManager):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
    
    def log_result(self, node: str, datafragment: DataFragment) -> None:
        client_id = datafragment.get_client_id()
        query_id = datafragment.get_query_id()
        df_id = datafragment.get_id()
        df_str = datafragment.to_str()
        id_log = f"{RECEIVED_ID} {client_id} {query_id} {df_id}"
        result_log = f"{RESULT} {node} {df_str}"
        self._add_logs([id_log, result_log])
    
    def log_query_ended(self, datafragment: DataFragment) -> None:
        client_id = datafragment.get_client_id()
        query_id = datafragment.get_query_id()
        df_id = datafragment.get_id()
        id_log = f"{RECEIVED_ID} {client_id} {query_id} {df_id}"
        ended_log = f"{QUERY_ENDED} {client_id} {query_id}"
        self._add_logs([id_log, ended_log])

    def log_result_sent(self, node: str) -> None:
        self._add_logs([f"{RESULT_SENT} {node}"])
