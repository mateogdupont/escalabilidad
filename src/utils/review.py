from typing import List
from datetime import datetime
import jsons

# + info here -> https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews?select=Books_rating.csv
class Review:

    def __init__(self, id: int, title: str, user_id: str, profile_name: str, helpfulness: str, score: float, time: int, summary: str, text: str) -> None:
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
        return jsons.dump(self)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Review':
        return jsons.load(json_str, cls)