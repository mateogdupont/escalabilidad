import os
from typing import List, Optional, Tuple
from utils.mom.mom import MOM
from utils.structs.book import Book
from utils.structs.data_fragment import DataFragment
import base64
import datetime
import re
import logging as logger

BOOK = "BOOK"               # <datafragment como str>
RECEIVED_ID = "RECEIVED_ID" # <client_id> <query_id> <df_id>
RESULT = "RESULT"           # <node> [<time>] <datafragment como str>
QUERY_ENDED = "QUERY_ENDED" # <client_id> <query_id>
RESULT_SENT = "RESULT_SENT" # <node>
IGNORE = "IGNORE"           # <client_id>

TITLE = "TITLE"
BOOK_STR = "BOOK_STR"

END_LOG = "END_LOG"

class BasicLogWriter:
    def __init__(self, file_path: str) -> None:
        self.original_file_path = file_path
        self.file_path = self._load_newest_file_path()
        self.file = open(self.file_path, "a+")

    def _generate_log_file_name(self):
        updated_date = datetime.datetime.now()
        date_with_format = updated_date.strftime("%Y-%m-%d_%H-%M-%S")
        if self.original_file_path.endswith('.log'):
            file_path = self.original_file_path[:-4]

        return f"{file_path}_{date_with_format}.log"

    def _load_newest_file_path(self):
        dir_path = os.path.dirname(self.original_file_path)
        files = os.listdir(dir_path)
        node_name = self.original_file_path.split('/')[-1].replace('.log', '')
        valid_files = []
        for file in files:
            if '_temp' not in file and node_name in file:
                match = re.search(r'\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}', file)
                if match:
                    valid_files.append((file, match.group(0)))
        
        valid_files.sort(key=lambda x: x[1], reverse=True)

        if valid_files:
            newest_file_name = valid_files[0][0]
            newest_file_path = os.path.join(dir_path, newest_file_name)
            print(f"Writer: Loading newest file: {newest_file_path}")
            return newest_file_path
        else:
            new_path= self._generate_log_file_name()
            print(f"Writer: No valid files found, using: {new_path}")
            return new_path
    
    def _add_logs(self, logs: List[str]) -> None:
        logs = '\n'.join(logs) + f"\n{END_LOG}\n"
        self.file.write(logs)
        self.file.flush()

    def close(self) -> None:
        self.file.close()
    
    def open(self) -> None:
        self.file_path = self._load_newest_file_path()
        self.file = open(self.file_path, "a+")
    
    def log_ignore(self, client_id: str) -> None:
        self._add_logs([f"{IGNORE} {client_id}"])
        logger.info(f"Ignoring client {client_id}")

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
    
