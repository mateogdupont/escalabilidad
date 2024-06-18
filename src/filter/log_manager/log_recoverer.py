from typing import Tuple
from utils.log_manager.basic_log_recoverer import *
from utils.structs.data_fragment import DataFragment

class LogRecoverer(BasicLogRecoverer):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        
