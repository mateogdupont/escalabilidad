import base64
import os
import time
from typing import List, Optional, Tuple
from utils.structs.book import Book
from utils.structs.data_fragment import DataFragment
import logging as logger
import datetime
import re

SEP = " "
END = "\n"
NONE = "None"
TITLE = "TITLE"
BOOK_STR = "BOOK_STR"

RECEIVED_ID_PARTS = 4
RESULT_PARTS = 4
QUERY_ENDED_PARTS = 3
RESULT_SENT_PARTS = 2
BOOK_PARTS = 2
IGNORE_PARTS = 2

BOOK = "BOOK" 
RECEIVED_ID = "RECEIVED_ID"
RESULT = "RESULT"
QUERY_ENDED = "QUERY_ENDED"
RESULT_SENT = "RESULT_SENT"
IGNORE = "IGNORE"

END_LOG = "END_LOG"

class LogRecovererError(Exception):
    pass

class UnknownLogType(LogRecovererError):
    pass

class ErrorProcessingLog(LogRecovererError):
    pass

class BasicLogRecoverer:
    def __init__(self, file_path: str) -> None:
        self.original_file_path = file_path
        self.file_path = self._load_newest_file_path()
        self.tmp_file_path =  None
        self.received_ids = {}
        self.results = {}
        self.ended_queries = {}
        self.sent_results = set()
        self.books = {}
        self.ignore_ids = set()
        self._recover_funcs = {
            RECEIVED_ID: self._process_received_id,
            RESULT: self._process_result,
            QUERY_ENDED: self._process_query_ended,
            RESULT_SENT: self._process_result_sent,
            BOOK: self._process_book,
            IGNORE: self._process_ignore
        }
    
    def _process_ignore(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) < IGNORE_PARTS:
            raise ErrorProcessingLog(f"Error processing log: {line}")
        _, client_id = parts
        self.ignore_ids.add(client_id)
        return True

    def _process_received_id(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) < RECEIVED_ID_PARTS:
            raise ErrorProcessingLog(f"Error processing log: {line}")
        _, client_id, query_id, df_id = parts
        if client_id in self.ignore_ids or query_id in self.ended_queries.get(client_id, set()):
            return False
        self.received_ids[client_id] = self.received_ids.get(client_id, {})
        self.received_ids[client_id][query_id] = self.received_ids[client_id].get(query_id, set())
        self.received_ids[client_id][query_id].add(df_id)
        return True
    
    def _process_result(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) < RESULT_PARTS:
            raise ErrorProcessingLog(f"Error processing log: {line}")
        node = parts[1]
        time = parts[2]
        start = line.find(parts[3])
        df_str = line[start:]
        # df = DataFragment.from_bytes(base64.b64decode(df_str.encode('utf-8')))
        df = DataFragment.from_str(df_str)
        if df.get_client_id() in self.ignore_ids or node in self.sent_results:
            return False
        time = float(time) if time != NONE else None
        if time is not None:
            self.results[node] = self.results.get(node, ([], time))
            self.results[node][0].append(df)
        else:
            self.results[node] = self.results.get(node, [])
            self.results[node].append((df, time) if time is not None else df)
        return True
    
    def _process_query_ended(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) < QUERY_ENDED_PARTS:
            raise ErrorProcessingLog(f"Error processing log: {line}")
        _, client_id, query_id = parts
        if client_id in self.ignore_ids:
            return False
        self.ended_queries[client_id] = self.ended_queries.get(client_id, set())
        self.ended_queries[client_id].add(query_id)
        return True #TODO: check if needed
    
    def _process_result_sent(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) < RESULT_SENT_PARTS:
            raise ErrorProcessingLog(f"Error processing log: {line}")
        _, node = parts
        if node in self.sent_results:
            return False
        self.sent_results.add(node)
        return True #TODO: check if needed
    
    def _process_book(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) < BOOK_PARTS:
            raise ErrorProcessingLog(f"Error processing log: {line}")
        start = line.find(parts[1])
        book_str = line[start:]
        # book = Book.from_bytes(base64.b64decode(book_str.encode('utf-8')))
        book = Book.from_str(book_str)
        self.books[book.get_title()] = book
        return True

    def recover_data(self) -> None:
        if not os.path.exists(self.file_path):
            return
        with open(self.file_path, "r") as file:
            lines = file.readlines()
            if not lines:
                return
            start = False
            for line in lines[::-1]:
                if not start and not line.startswith(END_LOG):
                    logger.info(f"Skipping line: {line}")
                    continue
                if line.startswith(END_LOG):
                    start = True
                    continue
                log_type = line.split(SEP)[0]
                if not log_type in self._recover_funcs:
                    raise UnknownLogType(f"Unknown log type: {log_type}")
                self._recover_funcs[log_type](line)

    def get_received_ids(self) -> dict:
        return self.received_ids
    
    def get_results(self) -> dict:
        return self.results
    
    def get_books(self) -> dict:
        return self.books
    
    def get_ended_queries(self) -> dict:
        return self.ended_queries
    
    def set_ended_queries(self, ended_queries: dict) -> None:
        self.ended_queries = ended_queries

    def rewrite_logs(self, event) -> None:
        # return
        logger.info("Rewriting logs")
        time1 = time.time()
        to_write = []
        self.tmp_file_path = self._generate_log_temp_file_name()
        with open(self.file_path, "r") as log:
            with open(self.tmp_file_path, "w") as temp:
                lines = log.readlines()
                if not lines:
                    return
                start = False
                for line in lines[::-1]:
                    if event.is_set():
                        return
                    if not start and not line.startswith(END_LOG):
                        continue
                    if line.startswith(END_LOG):
                        start = True
                        continue
                    log_type = line.split(SEP)[0]
                    if not log_type in self._recover_funcs:
                        msg = f"Unknown log type: {log_type}"
                        raise UnknownLogType(msg)
                    if self._recover_funcs[log_type](line):
                        to_write.append(line)
                to_write.reverse()
                to_write.append(END_LOG + END)
                temp.write(''.join(to_write))
        logger.info("Logs rewritten, it took %s seconds", round(time.time()-time1))

    def _generate_log_temp_file_name(self):
        updated_date = datetime.datetime.now()
        date_with_format = updated_date.strftime("%Y-%m-%d_%H-%M-%S")
        if self.original_file_path.endswith('.log'):
            file_path = self.original_file_path[:-4]

        return f"{file_path}_{date_with_format}_temp.log"
    
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
            print(f"Recoverer: Loading newest file: {newest_file_path}")
            return newest_file_path
        else:
            new_path= self._generate_log_temp_file_name()[:-9] + '.log'
            print(f"Recoverer: No valid files found, using: {new_path}")
            return new_path
        
    def _rename_temp_file(self):
        file_path_without_temp = self.tmp_file_path[:-9] + '.log'
        os.rename(self.tmp_file_path, file_path_without_temp)
        return file_path_without_temp

    def swap_files(self) -> None:
        logger.info("Renaming files")
        new_path = self._rename_temp_file()
        os.remove(self.file_path)
        self.file_path = new_path
        self.tmp_file_path = None
        logger.info("New path loaded")
    
    def get_ignore_ids(self) -> set:
        return self.ignore_ids
    
    def set_ignore_ids(self, ignore_ids: set) -> None:
        self.ignore_ids = ignore_ids
