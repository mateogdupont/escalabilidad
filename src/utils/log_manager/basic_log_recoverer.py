import base64
import os
from typing import List, Optional, Tuple
from utils.structs.book import Book
from utils.structs.data_fragment import DataFragment
import logging as logger
from atomicswap import swap

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

BOOK = "BOOK" 
RECEIVED_ID = "RECEIVED_ID"
RESULT = "RESULT"
QUERY_ENDED = "QUERY_ENDED"
RESULT_SENT = "RESULT_SENT"

END_LOG = "END_LOG"

# TODO: review parts, some fields can contain sep
# TODO: merge main
# TODO: manejo de lasts

class LogRecovererError(Exception):
    pass

class UnknownLogType(LogRecovererError):
    pass

class ErrorProcessingLog(LogRecovererError):
    pass

class BasicLogRecoverer:
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self.tmp_file_path = self.file_path.replace(".log", ".temp")
        self.received_ids = {}
        self.results = {}
        self.ended_queries = {}
        self.sent_results = set()
        self.books = {}
        self._recover_funcs = {
            RECEIVED_ID: self._process_received_id,
            RESULT: self._process_result,
            QUERY_ENDED: self._process_query_ended,
            RESULT_SENT: self._process_result_sent,
            BOOK: self._process_book
        }
    
    def _process_received_id(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) < RECEIVED_ID_PARTS:
            raise ErrorProcessingLog(f"Error processing log: {line}")
        _, client_id, query_id, df_id = parts
        if query_id in self.ended_queries.get(client_id, set()):
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
        if node in self.sent_results:
            return False
        df = DataFragment.from_bytes(base64.b64decode(df_str))
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
        self.ended_queries[client_id] = self.ended_queries.get(client_id, set())
        self.ended_queries[client_id].add(query_id)
        return True #TODO: check if needed
    
    def _process_result_sent(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) < RESULT_SENT_PARTS:
            raise ErrorProcessingLog(f"Error processing log: {line}")
        _, node = parts
        self.sent_results.add(node)
        return True #TODO: check if needed
    
    def _process_book(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) < BOOK_PARTS:
            raise ErrorProcessingLog(f"Error processing log: {line}")
        start = line.find(parts[1])
        book_str = line[start:]
        book = Book.from_bytes(base64.b64decode(book_str))
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

    def rewrite_logs(self) -> None:
        # return
        logger.info("Rewriting logs")
        to_write = []
        with open(self.file.name, "r") as log:
            with open(self.tmp_file_path, "w") as temp:
                lines = log.readlines()
                if not lines:
                    return
                start = False
                for line in lines[::-1]:
                    if not start and not line.startswith(END_LOG):
                        continue
                    if line.startswith(END_LOG):
                        start = True
                        continue
                    log_type = line.split(SEP)[0]
                    if not log_type in self._recover_funcs:
                        raise UnknownLogType(f"Unknown log type: {log_type}")
                    if self._recover_funcs[log_type](line):
                        to_write.append(line)
                to_write.reverse()
                temp.write(''.join(to_write))
        logger.info("Logs rewritten")
    
    def swap_files(self) -> None:
        return
        swap(self.file_path, self.tmp_file_path)
        os.remove(self.tmp_file_path)
    