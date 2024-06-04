from typing import Tuple
from utils.basic_log_manager import *
from utils.structs.data_fragment import DataFragment

RECEIVED_ID = "RECEIVED_ID"
RESULT = "RESULT"
QUERY_ENDED = "QUERY_ENDED"
RESULT_SENT = "RESULT_SENT"

class LogManager(BasicLogManager):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
    
    def log_result(self, next_steps: List[Tuple[DataFragment, str]]) -> None:
        if len(next_steps) == 0:
            return
        client_id = next_steps[0][0].get_client_id()
        query_id = next_steps[0][0].get_query_id()
        df_id = next_steps[0][0].get_id()
        logs = []
        id_log = f"{RECEIVED_ID} {client_id} {query_id} {df_id}"
        logs.append(id_log)
        for df, node in next_steps:
            df_str = df.to_str()
            result_log = f"{RESULT} {node} {client_id} {query_id} {df_id} {df_str}"
            logs.append(result_log)
        self._add_logs(logs)
    
    def log_query_ended(self, datafragment: DataFragment) -> None:
        client_id = datafragment.get_client_id()
        query_id = datafragment.get_query_id()
        df_id = datafragment.get_id()
        id_log = f"{RECEIVED_ID} {client_id} {query_id} {df_id}"
        ended_log = f"{QUERY_ENDED} {client_id} {query_id}"
        self._add_logs([id_log, ended_log])

    def log_result_sent(self, node: str) -> None:
        self._add_logs([f"{RESULT_SENT} {node}"])
