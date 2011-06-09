#!/usr/bin/python2.6

import task

class SqlTask(task.SentinelTask):
  """A SQL task."""

  def __init__(self, task_name, sql='', host='localhost', port=3306, db=None,
               user='root', passwd='', interval=30):
    task.SentinelTask.__init__(self, task_name, interval)
    self.host = host
    self.port = port
    self.user = user
    self.passwd = passwd
    self.db = db
    self.sql = sql
    self.results = None
