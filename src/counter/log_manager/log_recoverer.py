import base64
from typing import Tuple
from utils.log_manager.basic_log_recoverer import *
from utils.structs.data_fragment import DataFragment

TOP = "TOP"
AMOUNT = "AMOUNT"
PERCENTILE = "PERCENTILE"
GROUP_DATA = "GROUP_DATA"
VALUE = "VALUE"

COUNTED_DATA_PARTS = 4
COUNTED_DATA_SENT_PARTS = 3

COUNTED_DATA = "COUNTED_DATA"
COUNTED_DATA_SENT = "COUNTED_DATA_SENT"

class LogRecoverer(BasicLogRecoverer):
    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self.counted_data = {}
        self.counted_data_sent = {}
        self._recover_funcs.update({
            COUNTED_DATA: self._process_counted_data,
            COUNTED_DATA_SENT: self._process_counted_data_sent
        })
    
    def _process_counted_data(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) < COUNTED_DATA_PARTS:
            return False
        # _, client_id, query_id, count_info = parts
        client_id = parts[1]
        query_id = parts[2]
        start = line.find(parts[3])
        count_info = line[start:]
        if query_id in self.counted_data_sent.get(client_id, set()):
            return True
        count_info = eval(count_info)
        
        self.counted_data[client_id] = self.counted_data.get(client_id, {})
        self.counted_data[client_id][query_id] = self.counted_data[client_id].get(query_id, {})

        if TOP in count_info.keys():
            self._process_top(client_id, query_id, count_info)
        elif PERCENTILE in count_info.keys():
            self._process_percentile(client_id, query_id, count_info)
        elif "2" in count_info.keys():
            self._process_2(client_id, query_id, count_info)
        elif "1" in count_info.keys():
            self._process1(client_id, query_id, count_info)
        else:
            return False
        return True

    def _process1(self, client_id, query_id, count_info):
        group_data = count_info[GROUP_DATA]
        value = count_info[VALUE]
        if group_data not in self.counted_data[client_id][query_id].keys():
            self.counted_data[client_id][query_id][group_data] = set()
        self.counted_data[client_id][query_id][group_data].add(value)

    def _process_2(self, client_id, query_id, count_info):
        group_data = count_info[GROUP_DATA]
        value = count_info[VALUE]
        if group_data not in self.counted_data[client_id][query_id].keys():
            self.counted_data[client_id][query_id][group_data] = {"TOTAL": 0, "COUNT": 0}
        self.counted_data[client_id][query_id][group_data]["TOTAL"] += value
        self.counted_data[client_id][query_id][group_data]["COUNT"] += 1

    def _process_percentile(self, client_id, query_id, count_info):
        group_data = count_info[GROUP_DATA]
        percentile = count_info[PERCENTILE]
        value = count_info[VALUE]
        if group_data not in self.counted_data[client_id][query_id].keys():
            self.counted_data[client_id][query_id][group_data] = {"PERCENTILE": percentile, "VALUES": []}
        self.counted_data[client_id][query_id][group_data]["VALUES"].append(value)

    def _process_top(self, client_id, query_id, count_info):
        self.counted_data[client_id][query_id][TOP] = self.counted_data[client_id][query_id].get(TOP, [])
        df = DataFragment.from_bytes(base64.b64decode(count_info[TOP]))
        amount = count_info[AMOUNT]
        added = False
        if len(self.counted_data[client_id][query_id][TOP]) < amount:
            self.counted_data[client_id][query_id][TOP].append(df)
            added = True
        else:
            lowest = self.counted_data[client_id][query_id][TOP][0]
            if df.get_query_info().get_average() > lowest.get_query_info().get_average():
                self.counted_data[client_id][query_id][TOP][0] = df
                added = True
        if added:
            self.counted_data[client_id][query_id][TOP].sort(key=lambda x: x.get_query_info().get_average(), reverse=True)
    
    def _process_counted_data_sent(self, line: str) -> bool:
        parts = line.split(SEP)
        if len(parts) < COUNTED_DATA_SENT_PARTS:
            return False
        _, client_id, query_id = parts
        self.counted_data_sent[client_id] = self.counted_data_sent.get(client_id, set())
        self.counted_data_sent[client_id].add(query_id)
        return True
        
    def get_counted_data(self) -> dict:
        return self.counted_data
    