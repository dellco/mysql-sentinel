#!/usr/bin/python2.6

# Sentinel main

import collections
import logging
import os
import re
import Queue
import threading
import time
import MySQLdb
import MySQLdb.cursors

# Sentinel
import module
MODULE_PATH = 'modules'


class TaskAggregator(threading.Thread):
  """Monitors the task queue, dispatches jobs to the Task Runner pool.

  Args:
    result_queue: a queue to put completed tasks on
    num_task_runners: int, a number of task runners.
  """

  def __init__(self, result_queue, num_task_runners=10):
    threading.Thread.__init__(self)
    self.task_queue = Queue.Queue()
    self.task_runners = []
    for num in xrange(num_task_runners):
      self.task_runners.append(TaskRunner(result_queue))

  def PickTasksToRun(self):
    """Sends tasks to be run from the queue, requeues those not ready."""
    tasks = []
    run_tasks = []
    while not self.task_queue.empty():
      tasks.append(self.task_queue.get())

    for task in tasks:
      if task.run_time <= time.time():
        run_tasks.append(task)
      else:
        self.task_queue.put(task)

    return run_tasks

  def SendTasks(self, connection_params, tasks):
    """Sends the tasks to a free TaskRunner.

    Args:
      connection_params: (user, host, port)
      tasks: Task objects to execute using the given connection params.
    """

    free_runners = []
    while not free_runners:
      free_runners = [r for r in self.task_runners if not r.busy]
      time.sleep(5)
    free_runners.pop().RunTasks(connection_params, tasks)

  def run(self):
    while True:
      tasks = self.PickTasksToRun()
      task_dict = collections.defaultdict(list)
      for task in tasks:
        hash_key = (task.user, task.host, task.port)
        task_dict[hash_key].append(task)
      for connection_params, tasks in task_dict.items():
        self.SendTasks(connection_params, tasks)
        # Maybe there's a way for a module to get notified of every task sent?
        # self.EventManager.Event(SentTasksEvent, message=tasks)


class TaskRunner(threading.Thread):
  """Runs tasks that it receives from the aggregator."""

  def __init__(self, result_queue):
    threading.Thread.__init__(self)
    self.result_queue = result_queue
    self.busy = False

  def GetSqlHandle(self, (user, host, port)):
    """Creates a db connection."""

    conn = MySQLdb.connect(host=host, user=user, port=port,
                           cursorclass=MySQLdb.cursors.DictCursor)
    return conn.cursor()

  def RunSql(self, connection, sql):
    """Runs SQL against the connection provided."""
    try:
      connection.execute(sql)
      return connection.fetchall()
    except MySQLdb.OperationalError, e:
      logging.info('Query:\n%s\nGenerated error: %s' % (sql, e))
      return e

  def RunTasks(self, connection_params, tasks):
    """Takes list of tasks, executes and stores the results on the result queue.

    Args:
      connection_params: (user, host, port)
      tasks: Task objects to execute using the given connection params.
    """
    if self.busy:
      logging.error('Was passed to a busy task running. Exiting.')
    self.busy = True

    connection = self.GetSqlHandle(connection_params)
    for task in tasks:
      logging.debug('Task Runner: Executing %s on %s' % (task.name, task.host))
      task.execute_time = time.time()
      results = self.RunSql(connection, task.sql)
      if results:
        task.results = results
        task.completed_time = time.time()
        self.result_queue.put(task)
    self.busy = False
    connection.close()


class ResultsProcessor(threading.Thread):
  """Takes results off of the result queue and processes them."""

  def __init__(self, event_manager, result_store):
    threading.Thread.__init__(self)
    self.event_manager = event_manager
    self.result_store = result_store
    self.result_queue = Queue.Queue()

  def run(self):
    while True:
      if not self.result_queue.empty():
        task = self.result_queue.get()
        self.result_store.StoreTask(task)
        self.event_manager.SendEvent(task)


class EventManager(object):
  """Passes messages to task subscribers."""

  def __init__(self):
    self.subscriptions = []

  def UpdateSubscriptions(self, subscriptions):
    """Updates subscriptions."""
    self.subscriptions = subscriptions

  def SendEvent(self, task):
    for module in self.subscriptions[task.name]:
      logging.debug('EventMgr: Sending %s to %s' % (task.name, module.name))
      module.ReceiveEvent(task)


class ResultStore(object):
  """Stores data for completed tasks."""

  def __init__(self, task_history_len=5):
    self.task_history_len = task_history_len
    self.store = collections.defaultdict(list)

  def StoreTask(self, task):
    """Stores task data for the given amount of iterations."""

    self.store[task.name].append(task)
    if len(self.store[task.name]) > self.task_history_len:
      del self.store[task.name][0]

  def GetTaskDetails(self, task_name, details):
    """Returns a dict of task details for the task given.

    Args:
      task_name: str, a module.task name to lookup
      details: a list of task details to lookup

    Returns:
      A dict with the details as the keys { detail: value, }. It will create an
      empty dict { detail: 0 } for details not set.
    """
    if task_name in self.store:
      detail_dict = {}
      task = self.store[task_name][-1]
      for detail in details:
        detail_dict[detail] = getattr(task, detail)
      return detail_dict
    else:
      return dict(zip(details, [0]*len(details)))


class Sentinel(threading.Thread):
  """Main sentinel class."""

  def __init__(self, config_file, state_file, log):
    threading.Thread.__init__(self)
    self.modules = []
    self._loaded_modules = []
    self.subscriptions = collections.defaultdict(list)
    self.event_manager = EventManager()
    self.result_store = ResultStore()
    self.results_processor = ResultsProcessor(self.event_manager,
                                              self.result_store)
    self.task_aggregator = TaskAggregator(self.results_processor.result_queue)
    self.config = self._LoadConfigFile(config_file)
    self._LoadModules(self.config)

  def run(self):
    logging.info('Sentinel: All modules loaded.')
    self.task_aggregator.start()
    logging.info('Sentinel: TaskAggregator started.')
    self.results_processor.start()
    logging.info('Sentinel: Results processor started.')
    self._SetSubscriptions()

  def _LoadConfigFile(self, config_file):
    """Loads the config file.

    Args:
      config_file: relative path to the config file

    Returns:
      the config in python form
    """
    try:
      file = open(config_file, 'r')
      file_data = file.read()
    except IOError, e:
      print e
      return

    try:
      config = eval(file_data)
    except:
      print 'Error parsing config file.'

    return config

  def _SetSubscriptions(self):
    """Returns a dict of subscribers for all tasks."""
    for module in self.modules:
      for subscription in module.subscriptions:
        self.subscriptions[subscription].append(module)
    self.event_manager.UpdateSubscriptions(self.subscriptions)

  def _ModuleToFile(self, name):
    """Converts a module class name to the corresponding module file name.

    Args:
      name: string of module name (CamelCase)

    Returns:
      string: CamelCase -> camel_case
    """
    sub1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', sub1).lower()

  def _LoadModules(self, config):
    """Loads each module in the config.

    Note: The task_queue is available as a global to all modules.

    Args:
      config: used to establish where the modules are and what their names are
    """
    globals = { 'SentinelModule': module.SentinelModule, }
    for module_instance, module_config in config.iteritems():
      class_file = self._ModuleToFile(module_config['module'])
      filename = os.path.join(MODULE_PATH, '%s.py' % class_file)

      if class_file not in self._loaded_modules:
        data = open(filename).read()
        exec(data, globals)

      self._loaded_modules.append(module_config['module'])
      self._AddModule(globals[module_config['module']], module_instance,
                      module_config, self.task_aggregator.task_queue,
                      self.result_store)
      logging.info('Sentinel: Loaded %s (%s)' % (module_instance,
                                                 module_config['module']))

  def _AddModule(self, module_class, instance_name, config, queue, store):
    """Adds and instantiates the modules.

    Args:
      module_class: the name of the module class
      instance_name: the name of the instance
      config: the specific config for the passed instance
      queue: the task queue modules will add tasks to
      store: result store
    """
    new_module = module_class(instance_name, config, queue, store)
    self.modules.append(new_module)
    new_module.start()
