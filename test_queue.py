import unittest
from src.broker import Task 
from pickle import loads
class TestTask(unittest.TestCase):

    def add(self, a, b):
        return a + b

    def test_It_PicklesAndUnpicklesTaskAndArgs(self):
        task = Task(self.add, 1, 2)
        pickled = task.pickle()
        
        self.assertIsNotNone(pickled)

        unpickled = loads(pickled)
        self.assertEqual(unpickled.task, self.add)
        self.assertEqual(unpickled.args, (1, 2))

class TestTaskQueue(unittest.TestCase):

    def add(self, a, b):
        return a + b

    def test_It_Queues(self):
        pass

if __name__ == "__main__":
    unittest.main()