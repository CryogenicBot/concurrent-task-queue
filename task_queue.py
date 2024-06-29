from multiprocessing import Process, Queue, Manager
import pickle
from enum import Enum
import hashlib

class TaskStatus(Enum):
    SPAWNED = 0
    QUEUED = 1
    RUNNING = 2
    SUCCESS = 3
    FAILED = 4

class Task:
    def __init__(self, task: callable, *args):
        self.task = task
        self.args = args
        self.status = TaskStatus.QUEUED

    def run(self) -> TaskStatus:
        self.status = TaskStatus.RUNNING
        try:
            self.task(*self.args)
            self.status = TaskStatus.SUCCESS
        except Exception as e:
            self.status = TaskStatus.FAILED

        return self.status

    def pickle(self) -> bytes:
        return pickle.dumps(self)
    
    def hash_key(self) -> str:
        task_hash = hashlib.sha256(self.task).hexdigest()
        args_hash = hashlib.sha256(self.args).hexdigest()
        return hashlib.sha256(f"task:{task_hash}|args:{args_hash}").hexdigest()

class Broker:
    def __init__(self):
        self.manager = Manager()
        self.queue = self.manager.Queue()
        self.failed_queue = self.manager.Queue()
        self.success_queue = self.manager.Queue()
        self.task_map = self.manager.dict()

    def add_task(self, task: Task):
        pickled_task = task.pickle()
        self.queue.put(pickled_task)
        self.task_map[task.hash_key()] = pickled_task

    def get_task(self) -> Task | None:
        if self.queue.empty():
            return None
        return pickle.loads(self.queue.get())

    def run_task(self, task: Task):
        task_status = task.run()
        if task_status == TaskStatus.SUCCESS:
            self.success_queue.put(task)
        elif task_status == TaskStatus.FAILED:
            self.failed_queue.put(task)
        else:
            raise Exception("Task reported an unknown status.")
        

