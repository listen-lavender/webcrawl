#!/usr/bin/env python
# coding=utf-8
import time
import heapq
import beanstalkc
import threading
import cPickle as pickle

from .. import Logger
from ..character import unicode2utf8, json
from . import fid

DESCRIBE = {0:'ERROR', 1:'COMPLETED', 2:'WAIT', 'READY':10, 3:'RUNNING', 4:'RETRY', 5:'ABANDONED'}


class Queue(Logger):
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

    def funid(self, name, mid=None):
        if mid is None:
            return Queue.funids[fid(name)]
        else:
            Queue.funids[fid(name)] = mid

    def put(self, item):
        priority, name, times, args, kwargs, tid, ssid, version = item
        self.bc.put(pickle.dumps({'priority': priority, 'name':name,
                                'times': times, 'args': args, 'kwargs': kwargs, 'tid':tid, 'ssid':ssid, 'version':version}), priority=priority)
        Queue.conditions[self.tube]['event'].clear()

    def get(self, block=True, timeout=0):
        item = self.bc.reserve(timeout=timeout)
        if item:
            item.delete()
            item = pickle.loads(item.body)
            return item['priority'], self.funid(item['name']), item['name'], item['times'], tuple(item['args']), item['kwargs'], item['tid'], item['ssid'], item['version']
        else:
            return None

    def empty(self):
        return self.bc.stats_tube(self.tube)['current-jobs-ready'] == 0

    def copy(self):
        pass

    def task_done(self, item, force=False):
        if self.empty() or force:
            # if self.empty() or force:
            Queue.conditions[self.tube]['event'].set()

    def task_skip(self, item):
        if item is not None:
            tid, ssid, status, txt, create_time = item
            elapse = round(time.time() - create_time, 2)
            create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(create_time))
            self._print(tid=tid, ssid=ssid, 
                status=status, elapse=elapse, 
                txt=txt, create_time=create_time)
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

