#!/usr/bin/env python
# coding=utf-8

import heapq
import beanstalkc
import threading
import cPickle as pickle

from character import unicode2utf8
from . import MACADDRESS

try:
    from kokolog.aboutfile import modulename, modulepath
    from kokolog.prettyprint import logprint
except:
    def modulename(n):
        return None

    def modulepath(p):
        return None

    def logprint(n, p):
        def _wraper(*args, **kwargs):
            print(' '.join(args))
        return _wraper, None

_print, logger = logprint(modulename(__file__), modulepath(__file__))

DESCRIBE = {0:'ERROR', 1:'COMPLETED', 2:'WAIT', 'READY':10, 3:'RUNNING', 4:'RETRY', 5:'ABANDONED'}


class Queue(object):
    conditions = {}
    funids = {}

    def __init__(self, host='localhost', port=11300, tube='default', timeout=30, items=None, unfinished_tasks=None):
        self.bc = beanstalkc.Connection(host, port, connect_timeout=timeout)
        self.tube = tube
        self.bc.use(self.tube)
        self.bc.watch(self.tube)
        if self.tube in Queue.conditions:
            pass
        else:
            Queue.conditions[self.tube] = {'unfinished_tasks': unfinished_tasks or 0, 'event': threading.Event()}
            self.clear()
            Queue.conditions[self.tube]['event'].set()
        if items:
            for item in items:
                self.put(item)

    def funid(self, methodName, methodId=None):
        if methodId is None:
            return Queue.funids['%s-%s' % (MACADDRESS, methodName)]
        else:
            Queue.funids['%s-%s' % (MACADDRESS, methodName)] = methodId

    def put(self, item):
        priority, methodId, methodName, times, args, kwargs, tid = item
        self.bc.put(pickle.dumps({'priority': priority, 'methodId': methodId, 'methodName':methodName,
                                'times': times, 'args': args, 'kwargs': kwargs, 'tid':tid}), priority=priority)
        Queue.conditions[self.tube]['unfinished_tasks'] += 1
        Queue.conditions[self.tube]['event'].clear()

    def get(self, block=True, timeout=0):
        item = self.bc.reserve(timeout=timeout)
        if item:
            item.delete()
            item = pickle.loads(item.body)
            return (item['priority'], item['methodId'], item['methodName'], item['times'], tuple(item['args']), item['kwargs'], item['tid']), None
        else:
            return None

    def empty(self):
        return self.bc.stats_tube(self.tube)['current-jobs-ready'] == 0

    def copy(self):
        pass

    def task_done(self, item, force=False):
        if Queue.conditions[self.tube]['unfinished_tasks'] <= 0:
            raise ValueError('task_done() called too many times')
        Queue.conditions[self.tube]['unfinished_tasks'] -= 1
        if Queue.conditions[self.tube]['unfinished_tasks'] == 0 or force:
            # if self.empty() or force:
            Queue.conditions[self.tube]['event'].set()

    def join(self):
        Queue.conditions[self.tube]['event'].wait()

    def clear(self):
        while not self.empty():
            item = self.get(timeout=10)
            del item

    def rank(self, weight):
        pass

    def __repr__(self):
        return "<" + str(self.__class__).replace(" ", "").replace("'", "").split('.')[-1]

    def collect(self):
        if self.tube in Queue.conditions:
            del Queue.conditions[self.tube]

    def __del__(self):
        del self.bc


if __name__ == '__main__':
    pass

