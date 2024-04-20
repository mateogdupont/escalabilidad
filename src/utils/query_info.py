from typing import List
import jsons

class QueryInfo:

    def __init__(self, author: str, count: int, sentiment: float) -> None:
        self.author = author
        self.count = count
        self.sentiment = sentiment

    def to_json(self) -> str:
        return jsons.dump(self)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'QueryInfo':
        return jsons.load(json_str, cls)