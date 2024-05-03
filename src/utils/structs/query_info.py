from typing import List, Tuple
import jsonpickle
import logging as logger

class QueryInfo:

    def __init__(self) -> None:
        self.last = False
        # results
        self.author = None
        self.n_distinct = None
        self.average = None
        self.sentiment = None
        self.percentile = None
        # filter params
        self.filter_on = None
        self.contains = None
        self.min = None
        self.max = None
        self.top = None
        # counter params
        self.group_by = None
        self.count_distinct = None
        self.average_column = None
        self.percentile_column = None

    def to_json(self) -> str:
        return jsonpickle.encode(self)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'QueryInfo':
        return jsonpickle.decode(json_str)
    
    def clone(self) -> 'QueryInfo':
        new = QueryInfo()
        new.set_author(self.author)
        new.set_n_distinct(self.n_distinct)
        new.set_average(self.average)
        new.set_percentile(self.percentile)
        new.set_sentiment(self.sentiment)
        new.set_filter_params(self.filter_on, self.contains, self.min, self.max, self.top)
        new.set_counter_params(self.group_by, self.count_distinct, self.average_column, self.percentile_column)
        return new
    
    def set_author(self, author: str) -> None:
        self.author = author
    
    def get_author(self) -> str:
        return self.author
    
    def set_as_last(self) -> None:
        self.last = True
    
    def is_last(self) -> bool:
        return self.last
    
    def set_n_distinct(self, n_distinct: int) -> None:
        self.n_distinct = n_distinct

    def get_n_distinct(self) -> int:
        return self.n_distinct
    
    def set_average(self, average: float) -> None:
        self.average = average

    def get_average(self) -> float:
        return self.average
    
    def set_percentile(self, percentile: float) -> None:
        self.percentile = percentile

    def get_percentile(self) -> float:
        return self.percentile
    
    def set_sentiment(self, sentiment: float) -> None:
        self.sentiment = sentiment

    def get_sentiment(self) -> float:
        return self.sentiment
    
    def set_filter_params(self, filter_on: str, contains: str, min: int, max: int, top: Tuple[int, str]) -> None:
        self.filter_on = filter_on
        self.contains = contains
        self.min = min
        self.max = max
        self.top = top
        # logger.info(f"Filter params setted -> filter_on: {filter_on}, contains: {contains}, min: {min}, max: {max}, top: {top}")
        
    
    def get_filter_params(self) -> Tuple[str, str, int, int]:
        return self.filter_on, self.contains, self.min, self.max
    
    def filter_by_top(self) -> bool:
        return self.top is not None
    
    def set_counter_params(self, group_by: str, count_distinct: int, average_column: str, percentile: Tuple[int, str]) -> None:
        self.group_by = group_by
        self.count_distinct = count_distinct
        self.average_column = average_column
        self.percentile_column = percentile
        # logger.info(f"Counter params setted -> group_by: {group_by}, count_distinct: {count_distinct}, average_column: {average_column}, percentile_column: {percentile}")
    
    def get_counter_params(self) -> Tuple[str, int, str, Tuple[int, str]]:
        return self.group_by, self.count_distinct, self.average_column, self.percentile_column
    
    def get_result(self) -> List[str]:
        results = [self.n_distinct, self.average,self.sentiment, self.percentile]
        return [str(result) if result is not None else "" for result in results]