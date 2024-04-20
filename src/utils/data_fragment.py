from typing import List, Optional
import jsons
from utils.book import Book
from utils.review import Review
from utils.query_info import QueryInfo


class DataFragment:

    def __init__(self, querys: List[int], next_step: int, book: Optional[Book], review: Optional[Review]) -> None:
        self.querys = querys
        self.next_step = next_step
        self.book = book
        self.review = review
        self.query_info = None
    
    def to_json(self) -> str:
        return jsons.dump(self)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'DataFragment':
        return jsons.load(json_str, cls)
    
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
        if self.query_info is not None:
            raise Exception("Query info already setted")
        self.query_info = query_info
    
    def get_query_info(self) -> 'QueryInfo':
        return self.query_info