from typing import Tuple
from utils.basic_log_manager import *
from utils.structs.book import Book
from utils.structs.data_fragment import DataFragment

# Books logs
BOOK = "BOOK"                           # <book title> <book como str>
RECEIVED_ID = "RECEIVED_ID"             # <client_id> <query_id> <df_id>
SIDE_TABLE_UPDATE = "SIDE_TABLE_UPDATE" # <client_id> <query_id> <book title>
SIDE_TABLE_ENDED = "SIDE_TABLE_ENDED"   # <client_id> <query_id>

# Reviews logs
RECEIVED_ID = "RECEIVED_ID"             # <client_id> <query_id> <df_id>
RESULT = "RESULT"                       # <node> <time> <datafragment como str>
QUERY_ENDED = "QUERY_ENDED"             # <client_id> <query_id>
RESULT_SENT = "RESULT_SENT"             # <node>

class LogManager(BasicLogManager):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)

    def log_book(self, book: Book) -> None:
        book_title = book.get_title()
        book_str = book.to_str()
        self._add_logs([f"{BOOK} {book_title} {book_str}"])

    def log_side_table_update(self, fragment: DataFragment) -> None:
        client_id = fragment.get_client_id()
        query_id = fragment.get_query_id()
        df_id = fragment.get_id()
        book_title = fragment.get_book().get_title()
        id_log = f"{RECEIVED_ID} {client_id} {query_id} {df_id}"
        update_log = f"{SIDE_TABLE_UPDATE} {client_id} {query_id} {book_title}"
        self._add_logs([id_log, update_log])
    
    def log_side_table_ended(self, fragment: DataFragment) -> None:
        client_id = fragment.get_client_id()
        query_id = fragment.get_query_id()
        df_id = fragment.get_id()
        id_log = f"{RECEIVED_ID} {client_id} {query_id} {df_id}"
        ended_log = f"{SIDE_TABLE_ENDED} {client_id} {query_id}"
        self._add_logs([id_log, ended_log])