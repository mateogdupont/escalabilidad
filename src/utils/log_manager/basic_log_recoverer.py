from typing import List, Optional, Tuple
from utils.structs.book import Book
from utils.structs.data_fragment import DataFragment

SEP = " "
NONE = "None"
TITLE = "TITLE"
BOOK_STR = "BOOK_STR"

RECEIVED_ID_PARTS = 4
RESULT_PARTS = 6
QUERY_ENDED_PARTS = 3
RESULT_SENT_PARTS = 2
BOOK_PARTS = 2

class BasicLogRecoverer:
    def __init__(self, file_path: str) -> None:
        self.file_path = file_path
        self.receive_ids = {}
        self.results = {}
        self.ended_queries = {}
        self.sent_results = set()
        self.books = {}
    
    def _valid_line(self, line: str) -> bool:
        pass # la linea debe estar completa, el ult arg debe estar completo y ser válido
             # ver si las validaciones de largo son necesarias
    
    def _process_received_id(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) != RECEIVED_ID_PARTS:
            return False
        _, client_id, query_id, df_id = parts
        self.receive_ids[client_id] = self.receive_ids.get(client_id, {})
        self.received_ids[client_id][query_id] = self.received_ids[client_id].get(query_id, set())
        self.received_ids[client_id][query_id].add(df_id)
        return True
    
    def _process_result(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) != RESULT_PARTS:
            return False
        _, node, time, client_id, query_id, df_str = parts
        df = DataFragment.from_str(df_str)
        time = float(time) if time != NONE else None
        self.results[node] = self.results.get(node, [])
        self.results[node].append((df, time) if time is not None else df)
        return True
    
    def _process_query_ended(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) != QUERY_ENDED_PARTS:
            return False
        _, client_id, query_id = parts
        self.ended_queries[client_id] = self.ended_queries.get(client_id, set())
        self.ended_queries[client_id].add(query_id)
        return True
    
    def _process_result_sent(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) != RESULT_SENT_PARTS:
            return False
        _, node = parts
        self.sent_results.add(node)
        return True
    
    def _process_book(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) != BOOK_PARTS:
            return False
        _, book_str = parts
        book = Book.from_str(book_str)
        self.books[book.get_title()] = book
        return True

    
