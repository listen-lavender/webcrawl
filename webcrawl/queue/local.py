#!/usr/bin/env python
# coding=utf-8

import json
import heapq
import threading
import cPickle as pickle
from bson import ObjectId
from lib import queue
threading.queue = queue

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


# class Queue(threading.queue.Queue):

#     def __new__(cls):
#         return threading.queue.Queue.__new__(cls)
def Queue():

    def __init__(self, maxsize=None, items=None, unfinished_tasks=None):
        self.is_patch = not 'join' in dir(threading.queue.Queue)
        self.maxsize = maxsize or 0
        self.items = items

        self.parent = threading.queue.Queue.__init__(self, maxsize)
        if self.is_patch:
            from gevent.event import Event
            self._cond = Event()
            self._cond.set()

        if unfinished_tasks:
            self.unfinished_tasks = unfinished_tasks
        elif items:
            self.unfinished_tasks = len(items)
        else:
            self.unfinished_tasks = 0

        if self.is_patch and self.unfinished_tasks:
            self._cond.clear()

    def funid(self, methodName, methodId=None):
        if methodId is None:
            return self.funids['%s-%s' % (MACADDRESS, methodName)]
        else:
            self.funids['%s-%s' % (MACADDRESS, methodName)] = methodId

    def _init(self, maxsize):
        if self.items:
            self.queue = list(items)
        else:
            self.queue = []

    def _put(self, item, heappush=heapq.heappush):
        heappush(self.queue, item)
        if self.is_patch:
            self.unfinished_tasks += 1
            self._cond.clear()

    def _get(self, heappop=heapq.heappop):
        return heappop(self.queue), None

    def task_done(self, item, force=False):
        if self.is_patch:
            if self.unfinished_tasks <= 0:
                raise ValueError('task_done() called too many times')
            self.unfinished_tasks -= 1
            if self.unfinished_tasks == 0 or force:
                self._cond.set()
        else:
            self.all_tasks_done.acquire()
            try:
                unfinished = self.unfinished_tasks - 1
                if unfinished <= 0 or force:
                    if unfinished < 0:
                        raise ValueError('task_done() called too many times')
                    self.all_tasks_done.notify_all()
                self.unfinished_tasks = unfinished
            finally:
                self.all_tasks_done.release()

    def join(self):
        if self.is_patch:
            self._cond.wait()
        else:
            # self.parent.join()
            self.all_tasks_done.acquire()
            try:
                while self.unfinished_tasks:
                    self.all_tasks_done.wait()
            finally:
                self.all_tasks_done.release()

    def rank(self, weight):
        pass

    def collect(self):
        pass

    PriorityQueue = type('PriorityQueue', (threading.queue.Queue, ), {'__init__':__init__, '_init':_init, '_put':_put, '_get':_get, 'task_done':task_done, 'join':join, 'rank':rank, 'collect':collect})
    PriorityQueue.funids = {}

    return PriorityQueue


if __name__ == '__main__':
    pass

