from typing import Tuple
from utils.log_manager.basic_log_recoverer import *
from utils.structs.data_fragment import DataFragment

SIDE_TABLE_UPDATE_PARTS = 4
SIDE_TABLE_ENDED_PARTS = 3

class LogRecoverer(BasicLogRecoverer):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self.books_side_tables = {}
        self.side_tables_ended = {}

    def _process_side_table_update(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) != SIDE_TABLE_UPDATE_PARTS:
            return False
        _, client_id, query_id, book_title = parts
        self.books_side_tables[client_id] = self.books_side_tables.get(client_id, {})
        self.books_side_tables[client_id][query_id] = self.books_side_tables[client_id].get(query_id, set())
        self.books_side_tables[client_id][query_id].add(book_title)
        return True

    def _process_side_table_ended(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) != QUERY_ENDED_PARTS:
            return False
        _, client_id, query_id = parts
        self.side_tables_ended[client_id] = self.side_tables_ended.get(client_id, set())
        self.side_tables_ended[client_id].add(query_id)
        return True
    
    def recover_data(self) -> None:
        # read the file line by line from the end to the beginning
        # and process the lines with the corresponding methods
        pass
    