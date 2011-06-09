#!/usr/bin/python2.6

import logging
import threading
import time
import Queue

class SentinelModule(threading.Thread):
  """Base class for all modules."""

  def __init__(self, module_name, config, queue, store):
    threading.Thread.__init__(self)
    self.name = module_name
    self.module = config.get('module')
    self.task_queue = queue
    self.interval = None
    self.tasks = []
    self.subscriptions = self.AddSubscriptions(config)
    self.result_store = store

  def run(self):
    while True:
      if self.tasks:
        self.QueueTask(self.tasks.pop())

  def AddSubscriptions(self, config):
    """Digs through the config and adds addiotnal subscriptions if needed.

    Args:
      config: the pythonic config for the module
    """
    subscriptions = config.get('subscriptions', [])
    if 'interval' in config:
      if self.tasks:
        subscriptions.extend([task.name for task in self.tasks])
    return subscriptions

  def QueueTask(self, task):
    """Adds tasks to the Aggregator task_queue.

    Args:
      task: A task object.
    """
    logging.debug('Module %s: queueing %s' % (self.name, task.name))
    task.queue_time = time.time()
    self.task_queue.put(task)

  def GenerateTasks(self, hosts, tasks, name):
    """Creates a task for each host in hosts.

    Args:
      hosts: Each host to create tasks for
      tasks: Tasks to create
      name: a task name

    Returns:
      A list of Tasks.
    """
    raise NotImplementedError

  def AddTask(self, task_def):
    """Used to create task objects associated to the Module.

    Args:
      task_def: a data type that will be turned into a Task object

    Returns:
      Task: a Task object
    """
    raise NotImplementedError

  def ReceiveEvent(self, event):
    """Receives events from the event manager.

    Events are sent to modules which trigger modules to retrieve data from the
    ResultStore.

    Args:
      event: An event object
    """
    raise NotImplementedError
