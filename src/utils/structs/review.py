from typing import List, Optional
from datetime import datetime
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
    
    def to_json(self) -> str:
        return jsonpickle.encode(self)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Review':
        return jsonpickle.decode(json_str)
    
    def get_text(self) -> str:
        return self.text
    def get_book_title(self) -> str:
        return self.title
