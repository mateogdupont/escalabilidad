from typing import List, Optional
from datetime import datetime
import jsonpickle
import json

# + info here -> https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews?select=books_data.csv
class Book:
    
    def __init__(self, title: str, description: str, authors: List[str], image: Optional[str], preview_link: Optional[str], publisher: str, published_year: str, info_link: Optional[str], categories: List[str], ratings_count: float) -> None:
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

    def to_json(self) -> str:
        return jsonpickle.encode(self)

    @classmethod
    def from_json(cls, json_str: str) -> 'Book':
        return jsonpickle.decode(json_str)
    
    def has_minimun_data(self) -> bool:
        if not self.title or not self.authors or not self.categories:
            return False
        if self.published_year is None:
            return False
        return True
    
    def get_title(self) -> str:
        return self.title
    
    def get_authors(self) -> List[str]:
        return self.authors
    
    def get_publisher(self) -> str:
        return self.publisher
    
    def get_published_year(self) -> int:
        return int(self.published_year)
    
    def get_categories(self) -> List[str]:
        return self.categories
