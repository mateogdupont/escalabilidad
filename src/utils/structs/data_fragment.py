import base64
from typing import List, Optional, Tuple
import pickle
import re
from utils.structs.book import Book
from utils.structs.review import Review
from utils.structs.query_info import QueryInfo
from datetime import datetime

year_regex = re.compile('[^\d]*(\d{4})[^\d]*')

class DataFragment:

    def __init__(self, id: int, queries: 'dict[int, int]', book: Optional[Book], review: Optional[Review], client_id: str) -> None:
        self.id = id
        self.client_id = client_id
        self.book = book
        self.review = review
        self.query_info = QueryInfo()
        self.queries = None
        self.set_queries(queries)
        
        # last sync
        self.start_sync = False
        self.end_sync = False
    
    def set_sync(self, start: bool, end: bool) -> None:
        self.start_sync = start
        self.end_sync = end
    
    def get_sync(self) -> Tuple[bool, bool]:
        return (self.start_sync, self.end_sync)
    
    @classmethod
    def from_bytes(cls, bytes_obj: bytes) -> 'DataFragment':
        datafragment = pickle.loads(bytes_obj)
        datafragment.set_queries(datafragment.queries)
        return datafragment
    
    def to_bytes(self) -> bytes:
        return pickle.dumps(self)
    
    def to_str(self) -> str:
        _bytes = self.to_bytes()
        base64_encoded_bytes = base64.b64encode(_bytes)
        base64_string = base64_encoded_bytes.decode('utf-8')
        return base64_string

    @classmethod
    def from_str(cls, string: str) -> 'DataFragment':
        base64_encoded_bytes = string.encode('utf-8')
        binary_data = base64.b64decode(base64_encoded_bytes)
        return cls.from_bytes(binary_data)
        
    
    def to_human_readable(self) -> str:
        book = self.book.get_title() if self.book else "None"
        review = self.review.to_human_readable() if self.review else "None"
        query_info = self.query_info.to_human_readable()
        info = f"DataFragment (Id: {self.id} | Client Id: {self.client_id} | Queries: {self.queries})\n"
        info += f"- Book title: {book}\n"
        info += f"- Review: {review}\n"
        info += f"- Query Info: {query_info}"
        return info

    def get_id(self) -> int:
        return self.id
    
    # Creates a result array
    # ['last',Query','Title','Author','Publisher','Publised Year','Categories','Distinc Amount', 'Average', 'Sentiment', 'Percentile']
    def to_result(self) -> List[str]:
        book_result = [""] * 5
        query_info_results = [""] * 4
        query = str(list(self.get_queries().keys())[0])
        book = self.get_book()
        if book:
            book_result = book.get_result()
        query_info = self.get_query_info()
        if query_info:
            if book_result[1] == "":
                book_result[1] = query_info.get_author()
            query_info_results = query_info.get_result()

        return [str(int(self.is_last()))] + [query] + book_result + query_info_results
    
    # Review raw data:
    # last|book|Id|Title|Price|User_id|profileName|review/helpfulness|review/score|review/time|review/summary|review/text
    @classmethod
    def from_raw_review_data(cls, id:int, review_data: List[str], client_id: str,queries: 'dict[int, int]') -> 'DataFragment':
        review = Review(None,review_data[3],None,None,None,float(review_data[8]),None,None,review_data[11])
        if review.has_minimun_data():
            if 5 in queries and not review_data[11]:
                return None
            return DataFragment(id, queries.copy(),None , review, client_id)
        else:
            return None
        
    # Book db:
    # last|book|Title|description|authors|image|previewLink|publisher|pubishedDate|infoLink|categories|ratingCount
    @classmethod
    def from_raw_book_data(cls, id: int, book_data: List[str], client_id: str,queries: 'dict[int, int]') -> 'DataFragment':
        publish_year = None
        if book_data[8]:
            result = year_regex.search(book_data[8])
            publish_year = result.group(1) if result else None
    
        book = Book(book_data[2],None,book_data[4],None,None,book_data[7],publish_year,None,book_data[10],book_data[11])
        if book.has_minimun_data():
            return DataFragment(id, queries.copy(), book , None, client_id)
        else:
            return None
        
    
    def set_queries(self, queries: 'dict[int, int]') -> None:
        corrected = {}
        for key, value in queries.items():
            corrected[int(key)] = int(value)
        self.queries = corrected
    
    def get_client_id(self) -> str:
        return self.client_id
    
    def set_as_last(self) -> None:
        self.query_info.set_as_last()

    def set_as_clean_flag(self) -> None:
        self.query_info.set_as_clean_flag()
    
    def is_last(self) -> bool:
        return self.query_info.is_last()

    def get_queries(self) -> 'dict[int, int]':
        self.set_queries(self.queries)
        return self.queries
    
    def set_book(self, book: Book) -> None:
        if self.book is not None:
            raise Exception("Book already setted")
        self.book = book

    def set_review(self, review: Review) -> None:
        self.review = review
    
    def get_book(self) -> Book:
        return self.book
    
    def get_review(self) -> Review:
        return self.review
    
    def set_query_info(self, query_info) -> None:
        self.query_info = query_info
    
    def get_query_info(self) -> 'QueryInfo':
        if not self.query_info:
            self.query_info = QueryInfo()
        return self.query_info
    
    def clone(self) -> 'DataFragment':
        new = DataFragment(self.id, self.queries, self.book, self.review, self.client_id)
        if not self.query_info:
            self.query_info = QueryInfo()
        new.set_query_info(self.query_info.clone())
        if self.is_last():
            new.set_as_last()
        return new
    
    def get_query_id(self) -> str:
        ids = []
        for query, step in self.queries.items():
            ids.append(f"{query}:{step}")
        return "-".join(ids)