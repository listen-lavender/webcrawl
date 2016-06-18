#!/usr/bin/env python
# coding=utf-8
import time
import heapq
import threading
import cPickle as pickle
from bson import ObjectId

from lib import redis
from .. import Logger
from ..character import unicode2utf8, json
from . import fid

class Queue(Logger):
    conditions = {}

    def __init__(self, host='localhost', port=6379, db=0, tube='', timeout=30, items=None, unfinished_tasks=0, init=True, weight=[]):
        self.rc = redis.StrictRedis(host=host, port=port, db=db)
        self.prefix = 'pholcus_task'
        self.tube = '%s%s' % (self.prefix, tube)
        self.unfinished_tasks = 0

        if unfinished_tasks:
            self.unfinished_tasks = unfinished_tasks
        elif items:
            self.unfinished_tasks = len(items)
        else:
            self.unfinished_tasks = 0

        if self.tube in Queue.conditions:
            pass
        else:
            Queue.conditions[self.tube] = {'event': threading.Event(), 'mutex':threading.Lock(), 'weight':weight}
            Queue.conditions[self.tube]['event'].set()
        if init:
            self.clear()
        if items:
            for item in items:
                self.put(item)

    def funid(self, name, mid=None):
        if mid is None:
            return int(self.rc.hget('%s_funid' % self.tube, fid(name)) or 0)
        else:
            self.rc.hset('%s_funid' % self.tube, fid(name), mid)

    def put(self, item):
        priority, name, times, args, kwargs, tid, ssid, version = item
        # self.rc.zadd(self.tube, pickle.dumps({'priority': priority, 'mid': mid,
        #                         'times': times, 'args': args, 'kwargs': kwargs}), priority)
        self.rc.lpush('_'.join([self.tube, str(priority)]), pickle.dumps({'priority': priority, 'name':name, 'times': times, 'args': args, 'kwargs': kwargs, 'tid':tid, 'ssid':ssid, 'version':version}))
        self.unfinished_tasks += 1
        Queue.conditions[self.tube]['event'].clear()

    def get(self, block=True, timeout=0):
        # item = self.rc.zrangebyscore(self.tube, float('-inf'), float('+inf'), start=0, num=1)
        item = self.rc.brpop(['_'.join([str(self.tube), str(one)]) for one in Queue.conditions[self.tube]['weight']], timeout=timeout)
        if item:
            item = item[-1]
            item = pickle.loads(item)
            return item['priority'], self.funid(item['name']), item['name'], item['times'], tuple(item['args']), item['kwargs'], item['tid'], item['ssid'], item['version']

    def empty(self):
        total = sum([self.rc.llen(one) for one in ['_'.join([str(self.tube), str(one)]) for one in Queue.conditions[self.tube]['weight']]])
        return total == 0

    def copy(self):
        pass

    def task_done(self, item, force=False):
        if self.unfinished_tasks < 1 or force:
            Queue.conditions[self.tube]['event'].set()

    def task_skip(self, item):
        if item is not None:
            tid, ssid, status, txt, create_time = item
            elapse = round(time.time() - create_time, 2)
            create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(create_time))
            self._print(tid=tid, ssid=ssid, 
                status=status, elapse=elapse, 
                txt=txt, create_time=create_time)
        if self.unfinished_tasks < 1:
            Queue.conditions[self.tube]['event'].set()

    def join(self):
        Queue.conditions[self.tube]['event'].wait()

    def clear(self):
        for one in self.rc.keys():
            if one.startswith(self.prefix):
                self.rc.delete(one)

    def total(self):
        total = 0
        for one in self.rc.keys():
            if one.startswith(self.tube):
                total += self.rc.llen(one)
        return total

    def __repr__(self):
        return "<" + str(self.__class__).replace(" ", "").replace("'", "").split('.')[-1]

    def collect(self):
        if self.tube in Queue.conditions:
            del Queue.conditions[self.tube]

    def __del__(self):
        del self.rc


if __name__ == '__main__':
    pass

