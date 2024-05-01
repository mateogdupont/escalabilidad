from typing import List, Optional
import jsonpickle
from utils.structs.book import Book
from utils.structs.review import Review
from utils.structs.query_info import QueryInfo
from datetime import datetime


class DataFragment:

    def __init__(self, querys: 'dict[int, int]', book: Optional[Book], review: Optional[Review]) -> None:
        self.book = book
        self.review = review
        self.query_info = QueryInfo()
        self.queries = None
        self.set_queries(queries)
    
    def to_json(self) -> str:
        return jsonpickle.encode(self)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'DataFragment':
        datafragment = jsonpickle.decode(json_str)
        datafragment.set_queries(datafragment.queries)
        return datafragment
        
    
    def set_queries(self, queries: 'dict[int, int]') -> None:
        corrected = {}
        for key, value in queries.items():
            corrected[int(key)] = int(value)
        self.queries = corrected
    
    def set_as_last(self) -> None:
        self.query_info.set_as_last()
    
    def is_last(self) -> bool:
        return self.query_info.is_last()

    def get_querys(self) -> 'dict[int, int]':
        self.set_querys(self.querys)
        return self.querys
    
    def set_book(self, book: Book) -> None:
        if self.book is not None:
            raise Exception("Book already setted")
        self.book = book

    def set_review(self, review: Review) -> None:
        if self.review is not None:
            raise Exception("Review already setted")
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
        new = DataFragment(self.queries, self.book, self.review)
        if not self.query_info:
            self.query_info = QueryInfo()
        new.set_query_info(self.query_info.clone())
        return new
    
    def get_query_id(self) -> int:
        ids = []
        for query, step in self.queries.items():
            ids.append(f"{query}:{step}")
        return "-".join(ids)
