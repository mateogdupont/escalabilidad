from typing import List
import jsonpickle
from utils.structs.data_fragment import DataFragment


class DataChunk:

    def __init__(self, fragments: List[DataFragment]) -> None:
        self.amount = len(fragments)
        self.fragments = fragments
        self.contains_last = False
        for fragment in fragments:
            if fragment.is_last():
                self.contains_last = True
    
    def to_json(self) -> str:
        return jsonpickle.encode(self)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'DataChunk':
        datachunk = jsonpickle.decode(json_str, keys=True)
        return datachunk
        
    def contains_last_fragment(self) -> bool:
        return self.contains_last
    
    def set_fragments(self, fragments: list[DataFragment]) -> None:
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
