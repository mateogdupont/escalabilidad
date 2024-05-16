import ast
from typing import List, Optional
from datetime import datetime
# import jsonpickle # type: ignore
import pickle
import json
import logging as logger

# + info here -> https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews?select=books_data.csv
class Book:
    
    def __init__(self, title: str, description: Optional[str], authors: List[str], image: Optional[str], preview_link: Optional[str], publisher: str, published_year: str, info_link: Optional[str], categories: List[str], ratings_count: float) -> None:
        self.title = title
        self.description = description
        self.authors = authors
        self.image = image
        self.preview_link = preview_link
        self.publisher = publisher
        self.published_year = published_year
        self.info_link = info_link
        self.categories = categories
        self.ratings_count = ratings_count

    # def __repr__(self) -> str:
    #     return str({
    #         'title': self.title,
    #         'description': self.description,
    #         'authors': self.authors,
    #         'image': self.image,
    #         'preview_link': self.preview_link,
    #         'publisher': self.publisher,
    #         'published_year': self.published_year,
    #         'info_link': self.info_link,
    #         'categories': self.categories,
    #         'ratings_count': self.ratings_count
    #     })
    
    # @staticmethod
    # def from_repr(repr_str: str) -> 'Book':
    #     dict_repr = eval(repr_str)
    #     if dict_repr is None:
    #         return None
    #     book = Book(dict_repr['title'], dict_repr['description'], dict_repr['authors'], dict_repr['image'], dict_repr['preview_link'], dict_repr['publisher'], dict_repr['published_year'], dict_repr['info_link'], dict_repr['categories'], dict_repr['ratings_count'])
    #     return book

    def to_json(self) -> str:
        # return jsonpickle.encode(self)
        return pickle.dumps(self)

    @classmethod
    def from_json(cls, json_str: str) -> 'Book':
        # return jsonpickle.decode(json_str)
        return pickle.loads(json_str)
    
    def has_minimun_data(self) -> bool:
        if not self.title or not self.authors or not self.categories:
            return False
        if self.published_year is None:
            return False
        return True
    
    def get_title(self) -> str:
        return self.title
    
    def get_authors(self) -> List[str]:
        if isinstance(self.authors, str):
            self.authors = eval(self.authors)
        return self.authors
    
    def get_publisher(self) -> str:
        return self.publisher
    
    def get_published_year(self) -> int:
        return int(self.published_year)
    
    def get_categories(self) -> List[str]:
        if isinstance(self.categories, str):
            self.categories = eval(self.categories)
        return self.categories

    def get_result(self) -> List[str]:
        return [self.get_title(),self.get_authors(),self.get_publisher(),str(self.get_published_year()),self.get_categories()]