#!/usr/bin/env python
# coding=utf-8

import heapq
import beanstalkc
import threading
import cPickle as pickle

from ..character import unicode2utf8, json
from . import fid

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
            pass
        return _wraper, None

_print, logger = logprint(modulename(__file__), modulepath(__file__))

DESCRIBE = {0:'ERROR', 1:'COMPLETED', 2:'WAIT', 'READY':10, 3:'RUNNING', 4:'RETRY', 5:'ABANDONED'}


class Queue(object):
    conditions = {}
    funids = {}

    def __init__(self, host='localhost', port=11300, tube='', timeout=30, items=None, unfinished_tasks=None):
        self.bc = beanstalkc.Connection(host, port, connect_timeout=timeout)
        self.tube = 'pholcus_task%s' % tube
        self.bc.use(self.tube)
        self.bc.watch(self.tube)
        if self.tube in Queue.conditions:
            pass
        else:
            Queue.conditions[self.tube] = {'event': threading.Event()}
            self.clear()
            Queue.conditions[self.tube]['event'].set()
        if items:
            for item in items:
                self.put(item)

    def funid(self, methodName, methodId=None):
        if methodId is None:
            return Queue.funids[fid(methodName)]
        else:
            Queue.funids[fid(methodName)] = methodId

    def put(self, item):
        priority, methodName, times, args, kwargs, tid, sid, version = item
        self.bc.put(pickle.dumps({'priority': priority, 'methodName':methodName,
                                'times': times, 'args': args, 'kwargs': kwargs, 'tid':tid, 'sid':sid, 'version':version}), priority=priority)
        Queue.conditions[self.tube]['event'].clear()

    def get(self, block=True, timeout=0):
        item = self.bc.reserve(timeout=timeout)
        if item:
            item.delete()
            item = pickle.loads(item.body)
            return item['priority'], self.funid(item['methodName']), item['methodName'], item['times'], tuple(item['args']), item['kwargs'], item['tid'], item['sid'], item['version']
        else:
            return None

    def empty(self):
        return self.bc.stats_tube(self.tube)['current-jobs-ready'] == 0

    def copy(self):
        pass

    def task_done(self, item, force=False):
        if item is not None:
            args, kwargs, priority, sname, times, tid, sid, version = item
            _print('', tid=tid, sid=sid, version=version, type='COMPLETED', status=1, sname=sname, priority=priority, times=times, args='(%s)' % ', '.join([str(one) for one in args]), kwargs=json.dumps(kwargs, ensure_ascii=False), txt=None)
        if self.empty() or force:
            # if self.empty() or force:
            Queue.conditions[self.tube]['event'].set()

    def task_skip(self, item):
        if item is not None:
            tid, sid, count, sname, priority, times, args, kwargs, txt, version = item
            _print('', tid=tid, sid=sid, version=version, type='COMPLETED', status=0, sname=sname, priority=priority, times=times, args='(%s)' % ', '.join([str(one) for one in args]), kwargs=json.dumps(kwargs, ensure_ascii=False), txt=None)
        if self.empty():
            # if self.empty() or force:
            Queue.conditions[self.tube]['event'].set()

    def join(self):
        Queue.conditions[self.tube]['event'].wait()

    def clear(self):
        while not self.empty():
            item = self.bc.reserve(timeout=0)
            item.delete()
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

