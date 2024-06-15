import os
from typing import List, Optional, Tuple
from utils.mom.mom import MOM
from utils.structs.book import Book
from utils.structs.data_fragment import DataFragment
import base64

BOOK = "BOOK"               # <datafragment como str>
RECEIVED_ID = "RECEIVED_ID" # <client_id> <query_id> <df_id>
RESULT = "RESULT"           # <node> [<time>] <datafragment como str>
QUERY_ENDED = "QUERY_ENDED" # <client_id> <query_id>
RESULT_SENT = "RESULT_SENT" # <node>

TITLE = "TITLE"
BOOK_STR = "BOOK_STR"

END_LOG = "END_LOG"

class BasicLogWriter:
    def __init__(self, file_path: str) -> None:
        self.file = open(file_path, "a+")

    def _add_logs(self, logs: List[str]) -> None:
        logs = '\n'.join(logs) + f"\n{END_LOG}\n"
        self.file.write(logs)
        self.file.flush()

    def close(self) -> None:
        self.file.close()

    def log_result(self, next_steps: List[Tuple[DataFragment, str]], time: Optional[float] =None) -> None:
        if len(next_steps) == 0:
            return
        client_id = next_steps[0][0].get_client_id()
        query_id = next_steps[0][0].get_query_id()
        df_id = next_steps[0][0].get_id()
        logs = []
        id_log = f"{RECEIVED_ID} {client_id} {query_id} {df_id}"
        logs.append(id_log)
        for df, node in next_steps:
            df_str = base64.b64encode(df.to_bytes()).decode()
            result_log = f"{RESULT} {node} {time} {df_str}"
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
    
    def log_book(self, book: Book) -> None:
        self._add_logs([f"{BOOK} {base64.b64encode(book.to_bytes()).decode()}"])
