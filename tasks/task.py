#!/usr/bin/python2.6

import time
import logging

class SentinelTask:
  """Base class for all tasks."""

  def __init__(self, task_name, interval=0):
    self.name = task_name
    self.create_time = time.time()
    self.run_time = time.time() + interval
    self.queue_time = None
    self.execute_time = None
    self.completed_time = None
    self.return_data = None
