import unittest
from src.broker import Task, Broker, TaskStatus
import dill

class TestTask(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None 
    
    def add(self, a, b):
        return a + b

    def test_It_PicklesAndUnpicklesTaskAndArgs(self):
        task = Task(self.add, 1, 2)
        pickled = task.pickle()
        
        self.assertIsNotNone(pickled)

        unpickled = dill.loads(pickled)
        self.assertEqual(unpickled.task(*unpickled.args), self.add(1, 2))

    def test_It_Hashes(self):
        task = Task(self.add, 1, 2)
        task2 = Task(self.add, 1, 2)
        self.assertIsNotNone(task.hash_key(), task2.hash_key())

    def test_When_TasksHaveDifferentArgs_It_ReturnsDifferentHashKey(self):
        task = Task(self.add, 1, 2)
        task2 = Task(self.add, 1, 3)
        self.assertNotEqual(task.hash_key(), task2.hash_key())

    def test_When_TasksHaveDifferentStatusesButSameArgs_It_ReturnsSameHashKey(self):
        task = Task(self.add, 1, 2)
        task2 = Task(self.add, 1, 2)
        task2.status = TaskStatus.FAILED
        
        task.run()
        self.assertEqual(task.hash_key(), task2.hash_key())

    def test_When_TaskRunsSuccessfully_It_ReturnsSuccess(self):
        task = Task(self.add, 1, 2)
        self.assertEqual(task.run(), TaskStatus.SUCCESS)

    def test_When_TaskRunFails_It_ReturnsFailed(self):
        def fail():
            raise Exception("This is a test exception.")
        task = Task(fail)
        self.assertEqual(task.run(), TaskStatus.FAILED)

if __name__ == "__main__":
    unittest.main()