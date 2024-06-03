from typing import List


class BasicLogManager:
    def __init__(self, file_path: str) -> None:
        self.file = open(file_path, "a")

    def _add_logs(self, logs: List[str]) -> None:
        logs = "\n".join(logs) + "\n"
        self.file.write(logs)
        self.file.flush()

    def close(self) -> None:
        self.file.close()
