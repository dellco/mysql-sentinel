#!/usr/bin/python2.6

# Sentinel - This gets things moving

import sys
import logging
import threading

try:
  import argparse
except:
  import optparse as argparse

sys.path.append('modules/')
sys.path.append('tasks/')

import sentinel


def main(flags):

  log_format = '%(asctime)-15s %(message)s'
  logging.basicConfig(filename=flags.log, level=logging.DEBUG,
                      format=log_format)
  logging.info('Starting up the Sentinel.')
  s = sentinel.Sentinel(flags.config, flags.state_file, flags.log)
  s.start()
  logging.info('Started up the Sentinel.')

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('--config', action='store', dest='config', required=True,
                      help='Sentinel config file')
  parser.add_argument('--log', action='store', dest='log', required=False,
                      help='Sentinel log file')
  parser.add_argument('--state', action='store', dest='state_file',
                      required=False, help='Sentinel state file')
  main(parser.parse_args())
