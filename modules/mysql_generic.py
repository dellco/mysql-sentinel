#!/usr/bin/python2.6

import sql_module
import logging

class MysqlGeneric(sql_module.SqlModule):
  """A Sentinel module to gather general MySQL Data from a database."""

  def __init__(self, module_name, config, queue, store):
    sql_module.SqlModule.__init__(self, module_name, config, queue, store)
    self.name = module_name
    self.interval = config.get('interval', 30)
    self.tasks = self.GenerateTasks(self.hosts, config.get('queries'),
                                    self.interval)
    self.subscriptions = [task.name for task in self.tasks]

  def ReceiveEvent(self, event):
    logging.debug('Rescheduling %s on %s' % (event.name, event.host))
    self.tasks.extend(self.GenerateTasks(
        event.host, {event.name:event.sql}, self.interval, event.name))
