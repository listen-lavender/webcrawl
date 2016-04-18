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

DESCRIBE = {0:'ERROR', 1:'COMPLETED', 2:'WAIT', 'READY':10, 3:'RUNNING', 4:'RETRY', 5:'ABANDONED'}

class Queue(object):
    conditions = {}

    def __init__(self, host='localhost', port=6379, db=0, tube='', timeout=30, items=None, unfinished_tasks=None, init=True, weight=[]):
        self.rc = redis.StrictRedis(host='localhost', port=port, db=db)
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

    def sid(self):
        return str(ObjectId())

    def put(self, item):
        priority, methodName, times, args, kwargs, tid, sid = item
        # self.rc.zadd(self.tube, pickle.dumps({'priority': priority, 'methodId': methodId,
        #                         'times': times, 'args': args, 'kwargs': kwargs}), priority)
        exist = False
        if sid is not None:
            exist = self.rc.hget('pholcus_state', sid) is not None
        else:
            sid = str(ObjectId())
        if not exist:
            self.rc.lpush('_'.join([self.tube, str(priority)]), pickle.dumps({'priority': priority, 'methodName':methodName, 'times': times, 'args': args, 'kwargs': kwargs, 'tid':tid, 'sid':sid}))
            if times == 0:
                self.rc.hset('pholcus_state', sid, 2)
            else:
                self.rc.hset('pholcus_state', sid, 4)
            Queue.conditions[self.tube]['event'].clear()

    def get(self, block=True, timeout=0):
        # item = self.rc.zrangebyscore(self.tube, float('-inf'), float('+inf'), start=0, num=1)
        item = self.rc.brpop(['_'.join([str(self.tube), str(one)]) for one in Queue.conditions[self.tube]['weight']], timeout=timeout)
        if item:
            item = item[-1]
            item = pickle.loads(item)
            if self.rc.hget('pholcus_state', item['sid']) == 5:
                self.rc.hdel('pholcus_state', item['sid'])
                _print('', tid=item['tid'], sid=item['sid'], type='ABANDONED', status=2, sname='', priority=item['priority'], times=item['times'], args='(%s)' % ', '.join([str(one) for one in item['args']]), kwargs=json.dumps(item['kwargs'], ensure_ascii=False), txt=None)
                return None
            else:
                self.rc.hset('pholcus_state', item['sid'], 3)
                return item['priority'], self.funid(item['methodName']), item['methodName'], item['times'], tuple(item['args']), item['kwargs'], item['tid'], item['sid']
        else:
            return None

    def empty(self):
        total = sum([self.rc.llen(one) for one in ['_'.join([str(self.tube), str(one)]) for one in Queue.conditions[self.tube]['weight']]])
        return total == 0

    def copy(self):
        pass

    def task_done(self, item, force=False):
        if item is not None:
            args, kwargs, priority, sname, times, tid, sid = item
            _print('', tid=tid, sid=sid, type='COMPLETED', status=1, sname=sname, priority=priority, times=times, args='(%s)' % ', '.join([str(one) for one in args]), kwargs=json.dumps(kwargs, ensure_ascii=False), txt=None)
            self.rc.hdel('pholcus_state', sid)
        if self.empty() or force:
            # if self.empty() or force:
            Queue.conditions[self.tube]['event'].set()

    def join(self):
        Queue.conditions[self.tube]['event'].wait()

    def clear(self):
        for one in self.rc.keys():
            if one.startswith(self.prefix):
                self.rc.delete(one)
        self.rc.delete('pholcus_state')

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

    def abandon(self, sid):
        self.rc.hset('pholcus_state', sid, 5)

    def query(self, skip=0, limit=10):
        tubes = [one for one in self.rc.keys() if one.startswith(self.tube)]
        tubes.sort()
        result = []
        start = skip
        end = skip + limit - 1
        flag = False
        for tube in tubes:
            for item in self.rc.lrange(tube, start, end):
                item = pickle.loads(item)
                item['status_num'] = self.rc.hget('pholcus_state', item['sid']) or 3
                if len(result) + skip > DESCRIBE['READY']:
                    item['status_desc'] = DESCRIBE.get(int(item['status_num']))
                else:
                    item['status_desc'] = 'ready'
                result.append(item)
                if len(result) == limit:
                    flag = True
                    break
            else:
                start = 0
                end = limit - len(result) - 1
            if flag:
                break
        return result

    def __repr__(self):
        return "<" + str(self.__class__).replace(" ", "").replace("'", "").split('.')[-1]

    def collect(self):
        if self.tube in Queue.conditions:
            del Queue.conditions[self.tube]

    def __del__(self):
        del self.rc


if __name__ == '__main__':
    pass

