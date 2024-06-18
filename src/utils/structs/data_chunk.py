from typing import List
import pickle
from utils.structs.data_fragment import DataFragment
import logging as logger


class DataChunk:

    def __init__(self, fragments: 'list[DataFragment]') -> None:
        self.amount = len(fragments)
        self.fragments = fragments
        self.contains_last = False
        for fragment in fragments:
            if fragment.is_last():
                self.contains_last = True
    
    def to_bytes(self) -> bytes:
        return pickle.dumps(self)
    
    @classmethod
    def from_bytes(cls, json_str: bytes) -> 'DataChunk':
        return pickle.loads(json_str)
        
    def contains_last_fragment(self) -> bool:
        return self.contains_last
    
    def set_fragments(self, fragments: 'list[DataFragment]') -> None:
        self.fragments = fragments
        self.amount = len(fragments)
        self.contains_last = False
        for fragment in self.fragments:
            if fragment.is_last():
                self.contains_last = True

    def get_fragments(self) -> List[DataFragment]:
        return self.fragments
    
    def get_amount(self) -> int:
        return self.amount

    def add_fragment(self, fragment: DataFragment) -> None:
        self.fragments.append(fragment)
        self.amount += 1
        if fragment.is_last():
            self.contains_last = True
