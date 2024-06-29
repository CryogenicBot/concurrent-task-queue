
import unittest
from src.broker import Task, Broker, TaskStatus
import dill
import time

class TestTaskQueue(unittest.TestCase):
    def setUp(self):
        self.broker = Broker()

    def test_When_AddTaskCalled_It_QueuesTask(self):
        task = Task(add, 1, 2)
        self.broker.add_task(task)


        with self.subTest('it adds task to task_map'):
            self.assertIsNotNone(self.broker.task_map)
            self.assertEqual(len(self.broker.task_map), 1)
            self.assertEqual(self.broker.task_map[task.hash_key()], task.pickle())
            self.assertEqual(dill.loads(self.broker.task_map[task.hash_key()]).status, TaskStatus.QUEUED)

        with self.subTest('it adds task to queue'):
            self.assertIsNotNone(self.broker.queue)
            self.assertEqual(self.broker.queue.qsize(), 1)

            queued_task = dill.loads(self.broker.queue.get())
            self.assertIsNotNone(queued_task)
            self.assertEqual(task.args, queued_task.args)
            self.assertEqual(task.task(*task.args), queued_task.task(*queued_task.args))

    def test_When_GetTaskCalled_It_ReturnsTaskInFIFOOrder(self):
        task = Task(add, 1, 2)
        task2 = Task(sub, 2, 1)

        # task2 is added first
        self.broker.add_task(task2)
        self.broker.add_task(task)

        # task2 should be dequeued first
        queued_task2 = self.broker.get_task()
        queued_task = self.broker.get_task()

        self.assertIsNotNone(queued_task)
        self.assertIsNotNone(queued_task2)

        self.assertEqual(task.args, queued_task.args)
        self.assertEqual(task.task(*task.args), queued_task.task(*queued_task.args))
        self.assertEqual(task2.args, queued_task2.args)
        self.assertEqual(task2.task(*task2.args), queued_task2.task(*queued_task2.args))

    def test_When_RunTaskCalled_It_RunsTaskAndPutsItInAppropriateQueue(self): 
        task = Task(add, 1, 2)
        self.broker.add_task(task)
        task2 = Task(sub, 2, 1)
        self.broker.add_task(task2)
        task3 = Task(fail, 1, 2)
        self.broker.add_task(task3)

        processes = self.broker.run_tasks()
        for p in processes:
            p.join()


        self.assertEqual(self.broker.failed_queue.qsize(), 1)
        self.assertEqual(self.broker.success_queue.qsize(), 2)
        
        task_map = self.broker.get_task_map()
        finished_task = dill.loads(task_map[task.hash_key()])
        finished_task2 = dill.loads(task_map[task2.hash_key()])
        finished_task3 = dill.loads(task_map[task3.hash_key()])   

        # self.assertEqual(finished_task.status, TaskStatus.SUCCESS)
        # self.assertEqual(finished_task2.status, TaskStatus.SUCCESS)
        # self.assertEqual(finished_task3.status, TaskStatus.FAILED)


def add(a, b):
    return a + b

def sub(a, b):
    return a - b

def fail(_a, _b):
    raise Exception("This is a test exception.")

if __name__ == "__main__":
    unittest.main()