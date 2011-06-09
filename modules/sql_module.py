#!/usr/bin/python2.6

import sql_task
import module
import time

class SqlModule(module.SentinelModule):
  """SQL modules base class.

  Contains connection details for SQL based modules.
  """

  def __init__(self, module_name, config, queue, store):
    module.SentinelModule.__init__(self, module_name, config, queue, store)
    self.module_name = module_name
    self.user = 'root'
    self.passwd = ''
    self.hosts = config.get('hosts', ['localhost'])
    self.port = config.get('port', 3306)
    self.db = config.get('db', None)
    self.tasks = []

  def AddTask(self, name, sql, host, port, db, user, passwd, interval):
    """Used to create task objects associated to the Module.

    Args:
      task_def: a data type that will be turned into a Task object

    Returns:
      Task: a Task object
    """
    return sql_task.SqlTask(task_name=name, sql=sql, host=host,
                            port=port, db=db, user=user, passwd=passwd,
                            interval=interval)

  def GenerateTasks(self, hosts, query_dict, interval, name=None):
    """Creates a task for each host in hosts.

    Args:
      hosts: Each host to create tasks for (string or list)
      query_dict: A dict of name:query tasks to create {'name': 'sql'}

    Returns:
      A list of sql Tasks objects.
    """
    tasks = []

    if isinstance(hosts, str):
      hosts = [hosts]

    for host in hosts:
      for task_name, sql in query_dict.iteritems():
        tasks.append(self.AddTask(name=name or self.name+'.'+task_name, sql=sql,
                                  host=host, port=self.port, db=self.db, 
                                  user=self.user, passwd=self.passwd,
                                  interval=interval))
    return tasks
