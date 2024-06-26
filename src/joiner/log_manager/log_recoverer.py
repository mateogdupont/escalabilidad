from typing import Tuple
from utils.log_manager.basic_log_recoverer import *
from utils.structs.data_fragment import DataFragment

SIDE_TABLE_UPDATE_PARTS = 4
SIDE_TABLE_ENDED_PARTS = 3

SIDE_TABLE_UPDATE = "SIDE_TABLE_UPDATE"
SIDE_TABLE_ENDED = "SIDE_TABLE_ENDED"

class LogRecoverer(BasicLogRecoverer):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self.books_side_tables = {}
        self.side_tables_ended = {}
        self._recover_funcs.update({
            SIDE_TABLE_UPDATE: self._process_side_table_update,
            SIDE_TABLE_ENDED: self._process_side_table_ended
        })
        
    def _process_side_table_update(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) < SIDE_TABLE_UPDATE_PARTS:
            raise ErrorProcessingLog(f"Error processing log: {line}")
        client_id = parts[1]
        query_id = parts[2]
        if client_id in self.ignore_ids or query_id in self.ended_queries.get(client_id, set()):
            return False
        start = line.find(parts[3])
        book_title = line[start:]
        self.books_side_tables[client_id] = self.books_side_tables.get(client_id, {})
        self.books_side_tables[client_id][query_id] = self.books_side_tables[client_id].get(query_id, set())
        self.books_side_tables[client_id][query_id].add(book_title)
        return True

    def _process_side_table_ended(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) < QUERY_ENDED_PARTS:
            raise ErrorProcessingLog(f"Error processing log: {line}")
        _, client_id, query_id = parts
        if client_id in self.ignore_ids or query_id in self.ended_queries.get(client_id, set()):
            return False
        self.side_tables_ended[client_id] = self.side_tables_ended.get(client_id, set())
        self.side_tables_ended[client_id].add(query_id)
        return True
    
    def get_books_side_tables(self) -> dict:
        return self.books_side_tables
    
    def get_side_tables_ended(self) -> dict:
        return self.side_tables_ended
    