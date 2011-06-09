#!/usr/bin/python2.6

import sql_module
import logging

class MysqlSnipeLongQuery(sql_module.SqlModule):
  """A Sentinel module that issues kill statements for long running queries.

  Depends on mysql_generic.mysql_processlist
  Defaults to 1500 seconds.
  """

  def __init__(self, module_name, config, queue, store):
    sql_module.SqlModule.__init__(self, module_name, config, queue, store)
    self.name = module_name
    self.snipe_time = config.get('snipe_time', 1500)
    self.overrides = config.get('user_overrides', {})

  def AddKillTask(self, host, thread):
    """Issues tasks to kill long running queries.

    Args:
      host: the host to build the task for
      thread: thread id to kill

    Returns:
      Adds a task to self.tasks
    """
    self.tasks.extend(self.GenerateTasks(host, {'kill': 'kill %s' % thread},
                                         interval=0))

  def ReceiveEvent(self, event):
    """Proccesses the event received (process list) and issues kill statements.

    Args:
      event: An event task
    """
    for result in event.results:
      user_max_time = self.overrides.get(result['User'])
      if ((user_max_time and result['Time'] > user_max_time) or
          result['Time'] > self.snipe_time):
        logging.info('Killing user (%s) query on %s:\n%s' % (result['User'],
                                                             event.host,
                                                             event.sql))
        self.AddKillTask(event.host, result['Id'])
