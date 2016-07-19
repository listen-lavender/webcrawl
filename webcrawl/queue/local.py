#!/usr/bin/env python
# coding=utf-8
import time
import heapq
import threading
import cPickle as pickle
from bson import ObjectId

from lib import queue
from .. import Logger
from ..character import unicode2utf8, json
from . import fid

class Queue(queue.Queue, Logger):

    def __init__(self, maxsize=None, items=None, unfinished_tasks=None, weight=[]):
        self.maxsize = maxsize or 0
        self.items = items
        self.funids = {}

        self.parent = queue.Queue.__init__(self, maxsize)

        if unfinished_tasks:
            self.unfinished_tasks = unfinished_tasks
        elif items:
            self.unfinished_tasks = len(items)
        else:
            self.unfinished_tasks = 0

    def funid(self, name, mid=None):
        if mid is None:
            return self.funids[fid(name)]
        else:
            self.funids[fid(name)] = mid

    def _init(self, maxsize):
        if self.items:
            self.queue = list(items)
        else:
            self.queue = []

    def _put(self, item, heappush=heapq.heappush):
        heappush(self.queue, item)

    def _get(self, heappop=heapq.heappop):
        item = list(heappop(self.queue))
        item.insert(2, self.funid(item[2]))
        item = tuple(item)
        return item

    def task_done(self, item, force=False):
        self.all_tasks_done.acquire()
        try:
            unfinished = self.unfinished_tasks - 1
            if unfinished <= 0 or force:
                # if unfinished < 0:
                #     raise ValueError('task_done() called too many times')
                self.all_tasks_done.notify_all()
            self.unfinished_tasks = unfinished
        finally:
            self.all_tasks_done.release()

    def task_skip(self, item):
        if item is not None:
            tid, ssid, status, txt, create_time = item
            elapse = round(time.time() - create_time, 2)
            create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(create_time))
            self._print(tid=tid, ssid=ssid, 
                status=status, elapse=elapse, 
                txt=txt, create_time=create_time)

        self.all_tasks_done.acquire()
        try:
            unfinished = self.unfinished_tasks - 1
            if unfinished <= 0:
                # if unfinished < 0:
                #     raise ValueError('task_done() called too many times')
                self.all_tasks_done.notify_all()
            self.unfinished_tasks = unfinished
        finally:
            self.all_tasks_done.release()

    def join(self):
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


if __name__ == '__main__':
    pass

