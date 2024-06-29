
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
        broker = Broker()
        task = Task(add, 1, 2)
        broker.add_task(task)
        task2 = Task(sub, 2, 1)
        broker.add_task(task2)
        task3 = Task(fail, 1, 2)
        broker.add_task(task3)

        processes = broker.run_tasks()
        for p in processes:
            p.join()

        self.assertEqual(broker.failed_queue.qsize(), 1)
        self.assertEqual(broker.success_queue.qsize(), 2)
        
        failed_task = dill.loads(broker.failed_queue.get())
        successful_task1 = dill.loads(broker.success_queue.get())
        successful_task2 = dill.loads(broker.success_queue.get())

        self.assertEqual(failed_task.status, TaskStatus.FAILED)
        self.assertEqual(successful_task1.status, TaskStatus.SUCCESS)
        self.assertEqual(successful_task1.return_value, 3)
        self.assertEqual(successful_task2.status, TaskStatus.SUCCESS)
        self.assertEqual(successful_task2.return_value, 1)


def add(a, b):
    return a + b

def sub(a, b):
    return a - b

def fail(_a, _b):
    raise Exception("This is a test exception.")

if __name__ == "__main__":
    unittest.main()