#!/usr/bin/env python
# coding=utf-8

import json
import heapq
import threading
import cPickle as pickle
from bson import ObjectId

from .. import redis
from ..character import unicode2utf8
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
            print(' '.join(args))
        return _wraper, None

_print, logger = logprint(modulename(__file__), modulepath(__file__))

class Queue(object):
    conditions = {}

    def __init__(self, host='localhost', port=6379, db=0, tube='', timeout=30, items=None, unfinished_tasks=None, init=True, weight=[]):
        self.rc = redis.StrictRedis(host=host, port=port, db=db)
        self.prefix = 'pholcus_task'
        self.tube = '%s%s' % (self.prefix, tube)
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

    def funid(self, methodName, methodId=None):
        if methodId is None:
            return int(self.rc.hget('%s_funid' % self.tube, fid(methodName)) or 0)
        else:
            self.rc.hset('%s_funid' % self.tube, fid(methodName), methodId)

    def put(self, item):
        priority, methodName, times, args, kwargs, tid, sid, version = item
        # self.rc.zadd(self.tube, pickle.dumps({'priority': priority, 'methodId': methodId,
        #                         'times': times, 'args': args, 'kwargs': kwargs}), priority)
        self.rc.lpush('_'.join([self.tube, str(priority)]), pickle.dumps({'priority': priority, 'methodName':methodName, 'times': times, 'args': args, 'kwargs': kwargs, 'tid':tid, 'sid':sid, 'version':version}))
        Queue.conditions[self.tube]['event'].clear()

    def get(self, block=True, timeout=0):
        # item = self.rc.zrangebyscore(self.tube, float('-inf'), float('+inf'), start=0, num=1)
        item = self.rc.brpop(['_'.join([str(self.tube), str(one)]) for one in Queue.conditions[self.tube]['weight']], timeout=timeout)
        if item:
            item = item[-1]
            item = pickle.loads(item)
            return item['priority'], self.funid(item['methodName']), item['methodName'], item['times'], tuple(item['args']), item['kwargs'], item['tid'], item['sid'], item['version']

    def empty(self):
        total = sum([self.rc.llen(one) for one in ['_'.join([str(self.tube), str(one)]) for one in Queue.conditions[self.tube]['weight']]])
        return total == 0

    def copy(self):
        pass

    def task_done(self, item, force=False):
        if item is not None:
            args, kwargs, priority, sname, times, tid, sid = item
            _print('', tid=tid, sid=sid, type='COMPLETED', status=1, sname=sname, priority=priority, times=times, args='(%s)' % ', '.join([str(one) for one in args]), kwargs=json.dumps(kwargs, ensure_ascii=False), txt=None)
        if self.empty() or force:
            Queue.conditions[self.tube]['event'].set()

    def join(self):
        Queue.conditions[self.tube]['event'].wait()

    def clear(self):
        for one in self.rc.keys():
            if one.startswith(self.prefix):
                self.rc.delete(one)

    def rank(self, weight):
        Queue.conditions[self.tube]['mutex'].acquire()
        Queue.conditions[self.tube]['weight'].extend(weight)
        Queue.conditions[self.tube]['weight'].sort()
        Queue.conditions[self.tube]['mutex'].release()

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

