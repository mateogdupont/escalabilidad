import os
from typing import List, Optional, Tuple
from utils.mom.mom import MOM
from utils.structs.data_fragment import DataFragment

HIGH = 3
MEDIUM = 2
LOW = 1

RECEIVED_ID = "RECEIVED_ID" # <client_id> <query_id> <df_id>
RESULT = "RESULT"           # <node> [<time>] <datafragment como str>
QUERY_ENDED = "QUERY_ENDED" # <client_id> <query_id>
RESULT_SENT = "RESULT_SENT" # <node>

RECEIVED_ID_PRIORITY = LOW
RESULT_PRIORITY = LOW
QUERY_ENDED_PRIORITY = MEDIUM
RESULT_SENT_PRIORITY = MEDIUM

class BasicLogWriter:
    def __init__(self, log_queue: str, routing_key: str) -> None:
        self.mom = MOM({log_queue: True})
        self.queue_name = log_queue
        self.routing_key = routing_key

    def _add_logs(self, logs: dict) -> None:
        for log, priority in logs.items():
            if priority == LOW:
                self.mom.publish_log(self.queue_name, log, priority)
            else:
                self.mom.publish_log_global(self.routing_key, log, priority)

    def close(self) -> None:
        pass

    def log_result(self, next_steps: List[Tuple[DataFragment, str]], time: Optional[float] =None) -> None:
        if len(next_steps) == 0:
            return
        client_id = next_steps[0][0].get_client_id()
        query_id = next_steps[0][0].get_query_id()
        df_id = next_steps[0][0].get_id()
        logs = {}
        id_log = f"{RECEIVED_ID} {client_id} {query_id} {df_id}"
        logs[id_log] = RECEIVED_ID_PRIORITY
        for df, node in next_steps:
            df_str = df.to_str()
            result_log = f"{RESULT} {node} {time} {client_id} {query_id} {df_id} {df_str}"
            logs[result_log] = RESULT_PRIORITY
        self._add_logs(logs)
    
    def log_query_ended(self, datafragment: DataFragment) -> None:
        client_id = datafragment.get_client_id()
        query_id = datafragment.get_query_id()
        df_id = datafragment.get_id()
        id_log = f"{RECEIVED_ID} {client_id} {query_id} {df_id}"
        ended_log = f"{QUERY_ENDED} {client_id} {query_id}"
        logs = {id_log: RECEIVED_ID_PRIORITY, ended_log: QUERY_ENDED_PRIORITY}
        self._add_logs(logs)
    
    def log_result_sent(self, node: str) -> None:
        self._add_logs({f"{RESULT_SENT} {node}": RESULT_SENT_PRIORITY})
