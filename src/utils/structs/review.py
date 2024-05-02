from typing import List, Optional
from datetime import datetime
import logging as logger
import jsonpickle

# + info here -> https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews?select=Books_rating.csv
class Review:

    def __init__(self, id: int, title: str, user_id: Optional[str], profile_name: Optional[str], helpfulness: str, score: float, time: Optional[int], summary: str, text: str) -> None:
        self.id = id
        self.title = title
        self.user_id = user_id
        self.profile_name = profile_name
        self.helpfulness = helpfulness
        self.score = score
        self.time = time
        self.summary = summary
        self.text = text
    
    @classmethod
    def with_minimum_data(cls, title: str, text: str, score: float) -> 'Review':
        return cls(-1, title, None, None, None, score, None, None, text)
    
    def to_json(self) -> str:
        return jsonpickle.encode(self)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Review':
        return jsonpickle.decode(json_str)
    
    def has_minimun_data(self) -> bool:
        try:
            self.score = float(self.score) # if there was a header, this will fail
        except:
            return False
        return bool(self.title and self.text)
    
    def get_text(self) -> str:
        return self.text
    
    def get_book_title(self) -> str:
        return self.title
    
    def get_score(self) -> float:
        if type(self.score) == str:
            self.score = float(self.score)
        return self.score
