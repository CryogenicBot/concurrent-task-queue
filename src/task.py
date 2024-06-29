
from enum import Enum
import hashlib
import dill

class TaskStatus(Enum):
    SPAWNED = 0
    QUEUED = 1
    RUNNING = 2
    SUCCESS = 3
    FAILED = 4

class Task:

    def __init__(self, task: callable, *args):
        self.task: callable = task
        self.args = args
        self.status = TaskStatus.QUEUED
        self.return_value: any | None = None

    def run(self):
        self.status = TaskStatus.RUNNING
        try:
            self.return_value = self.task(*self.args)
            self.status = TaskStatus.SUCCESS
        except Exception as e:
            self.status = TaskStatus.FAILED


    def pickle(self) -> bytes:
        return dill.dumps(self)
    
    def hash_key(self) -> str:
        task_hash = hashlib.sha256(str(self.task).encode('utf-8')).hexdigest()
        args_list = [str(arg) for arg in self.args]
        args_hash = hashlib.sha256((",".join(args_list)).encode('utf-8')).hexdigest()
        return hashlib.sha256(f"task:{task_hash}|args:{args_hash}".encode('utf-8')).hexdigest()