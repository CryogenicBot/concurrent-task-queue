This message broker is based on the `multiprocessing` module. There are two main components: `Broker` and `Task`.

# Broker

The `Broker` class has public methods to add tasks to a queue, remove tasks from the queue, and run tasks.

## Adding Tasks

The task will be added to a thread-safe queue (initialized from the `Manager` class from the `multiprocessing` module) by "pickling" the object. The pickling is done via `dill` which is built on top of the native `pickle` module in python.

## Running Tasks

There are two ways of running tasks:
1. The consumer may call `run_task` with a `Task` object
2. Or the consumer can call `run_tasks` which will spawn child processes for every task in the current queue

Once a task is run, it will set the appropriate status and return value in the `status` and `return_value` attributes respectively. These attributes can then be retrieved once the task is removed from the appropriate `success` or `failed` queues in the broker.

# Sample Execution

```
broker = Broker()
broker.add_task(Task(print, "hello world"))
broker.add_task(Task(print, "bye world"))
broker.run_tasks()

# Output
>>> hello world
>>> bye world
```

# Installation & Running Tests
```
pip install -r requirements. txt
python -m test.test_broker # broker tests
python -m test.test_task # task tests
```

# Problems and Future Improvements
1. The queueing and dequeueing of the task causes the hash key to change, resulting in the task map containing copies of the task rather than updating the original entry. I ran out of time before I could fully investigate the issue
2. I would like to use pipes in order to have child processes communicate the execution of a task, and its end result. This would then become the basis of updating queues and task status.
