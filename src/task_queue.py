from multiprocessing import Process, Queue, Manager
from .task import Task, TaskStatus
import dill

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
        return dill.loads(self.queue.get())

    def run_task(self, task: Task):
        task_status = task.run()
        
        if task_status == TaskStatus.SUCCESS:
            self.success_queue.put(task)
        elif task_status == TaskStatus.FAILED:
            self.failed_queue.put(task)
        else:
            raise Exception("Task reported an unknown status.")
        

