from typing import List
# import jsonpickle
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

    # def __repr__(self) -> str:
    #     dict_repr = {
    #         'amount': self.amount,
    #         'fragments': [repr(fragment) for fragment in self.fragments],
    #         'contains_last': self.contains_last
    #     }
    #     return str(dict_repr)
    
    # @staticmethod
    # def from_repr(repr_str: str) -> 'DataChunk':
    #     dict_repr = eval(repr_str)
    #     fragments = [DataFragment.from_repr(fragment_repr) for fragment_repr in dict_repr['fragments']]
    #     data_chunk = DataChunk(fragments)
    #     data_chunk.amount = dict_repr['amount']
    #     data_chunk.contains_last = dict_repr['contains_last']
    #     return data_chunk
    
    def to_json(self) -> str:
        # return jsonpickle.encode(self)
        return pickle.dumps(self)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'DataChunk':
        # datachunk = jsonpickle.decode(json_str, keys=True)
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
