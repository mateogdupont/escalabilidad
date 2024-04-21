from typing import List
import jsonpickle

class QueryInfo:

    def __init__(self, author: str, count: int, sentiment: float) -> None:
        self.author = author
        self.count = count
        self.sentiment = sentiment

    def to_json(self) -> str:
        return jsonpickle.encode(self)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'QueryInfo':
        return jsonpickle.decode(json_str)