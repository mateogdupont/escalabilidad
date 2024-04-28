from typing import List, Optional
import jsonpickle
from utils.structs.book import Book
from utils.structs.review import Review
from utils.structs.query_info import QueryInfo
from datetime import datetime


class DataFragment:

    def __init__(self, querys: dict[int, int], book: Optional[Book], review: Optional[Review]) -> None:
        self.book = book
        self.review = review
        self.query_info = QueryInfo()
        self.querys = None
        self.set_querys(querys)
    
    def to_json(self) -> str:
        return jsonpickle.encode(self)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'DataFragment':
        datafragment = jsonpickle.decode(json_str)
        datafragment.set_querys(datafragment.querys)
        return datafragment
        
    
    def set_querys(self, querys: dict[int, int]) -> None:
        corrected = {}
        for key, value in querys.items():
            corrected[int(key)] = int(value)
        self.querys = corrected
    
    def set_as_last(self) -> None:
        self.query_info.set_as_last()
    
    def is_last(self) -> bool:
        return self.query_info.is_last()

    def get_querys(self) -> dict[int, int]:
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
        new = DataFragment(self.querys, self.book, self.review)
        if not self.query_info:
            self.query_info = QueryInfo()
        new.set_query_info(self.query_info.clone())
        return new

# datafragment = DataFragment([1, 2, 3], 1, None, None)
# print(datafragment.to_json())
# print(DataFragment.from_json(datafragment.to_json()).to_json())

# book = Book("title", "description", ["author1", "author2"], "image", "preview_link", "publisher", datetime.now(), "info_link", ["category1", "category2"], 1.0)
# print(book.to_json())
# print(Book.from_json(book.to_json()).to_json())

# datafragment.set_book(book)
# print(datafragment.to_json())
# print(DataFragment.from_json(datafragment.to_json()).to_json())

# review = Review(1, "title", "user_id", "profile_name", "helpfulness", 1.0, 1, "summary", "text")
# print(review.to_json())
# print(Review.from_json(review.to_json()).to_json())

# datafragment.set_review(review)
# print(datafragment.to_json())
# print(DataFragment.from_json(datafragment.to_json()).to_json())

# query_info = QueryInfo("author", 1, 1.0)
# print(query_info.to_json())
# print(QueryInfo.from_json(query_info.to_json()).to_json())

# datafragment.set_query_info(query_info)
# print(datafragment.to_json())
# print(DataFragment.from_json(datafragment.to_json()).to_json())
