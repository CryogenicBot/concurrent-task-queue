from multiprocessing import Process, Manager
from .task import Task, TaskStatus
import dill

class Broker:
    def __init__(self):
        self.manager = Manager()
        self.queue = self.manager.Queue()
        self.failed_queue = self.manager.Queue()
        self.success_queue = self.manager.Queue()
        self.task_map = self.manager.dict()

    def get_task_map(self):
        return self.task_map

    def add_task(self, task: Task):
        pickled_task = task.pickle()
        self.queue.put(pickled_task)
        self.task_map[task.hash_key()] = pickled_task

    def get_task(self) -> Task | None:
        if self.queue.empty():
            return None
        return dill.loads(self.queue.get())

    def run_task(self, task: Task) -> Process:
        def run():
            task.run()

            pickled_task = task.pickle()
            if task.status == TaskStatus.SUCCESS:
                self.success_queue.put(pickled_task)
            elif task.status == TaskStatus.FAILED:
                self.failed_queue.put(pickled_task)
            else:
                raise Exception("Task reported an unknown status.")

            self.task_map[task.hash_key()] = pickled_task

        process = Process(target=run, args=())
        process.start()

        return process

    def run_tasks(self):
        processes = []
        while not self.queue.empty():
            task = self.get_task()
            processes.append(self.run_task(task))

        return processes
        
        

