from typing import List, Optional
import pickle
import re
from utils.structs.book import Book
from utils.structs.review import Review
from utils.structs.query_info import QueryInfo
from datetime import datetime

year_regex = re.compile('[^\d]*(\d{4})[^\d]*')

class DataFragment:

    def __init__(self, queries: 'dict[int, int]', book: Optional[Book], review: Optional[Review], client_id: str) -> None:
        self.client_id = client_id
        self.book = book
        self.review = review
        self.query_info = QueryInfo()
        self.queries = None
        self.set_queries(queries)
    
    def to_str(self) -> str:
        return pickle.dumps(self)
    
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
    
    @classmethod
    def from_str(cls, json_str: str) -> 'DataFragment':
        datafragment = pickle.loads(json_str)
        datafragment.set_queries(datafragment.queries)
        return datafragment
    
    # Review raw data:
    # last|book|Id|Title|Price|User_id|profileName|review/helpfulness|review/score|review/time|review/summary|review/text
    @classmethod
    def from_raw_review_data(cls, review_data: List[str], client_id: str,queries: 'dict[int, int]') -> 'DataFragment':
        review = Review(None,review_data[3],None,None,None,float(review_data[8]),None,None,review_data[11])
        if review.has_minimun_data():
            if 5 in queries and not review_data[11]:
                return None
            return DataFragment(queries.copy(),None , review, client_id)
        else:
            return None
        
    # Book db:
    # last|book|Title|description|authors|image|previewLink|publisher|pubishedDate|infoLink|categories|ratingCount
    @classmethod
    def from_raw_book_data(cls, book_data: List[str], client_id: str,queries: 'dict[int, int]') -> 'DataFragment':
        publish_year = None
        if book_data[8]:
            result = year_regex.search(book_data[8])
            publish_year = result.group(1) if result else None
    
        book = Book(book_data[2],None,book_data[4],None,None,book_data[7],publish_year,None,book_data[10],book_data[11])
        if book.has_minimun_data():
            return DataFragment(queries.copy(), book , None, client_id)
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
        new = DataFragment(self.queries, self.book, self.review, self.client_id)
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