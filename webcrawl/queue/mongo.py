#!/usr/bin/env python
# coding=utf-8

import json
import heapq
import pymongo
import threading
import cPickle as pickle
from bson import ObjectId

from ..character import unicode2utf8
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

    def __init__(self, host='localhost', port=27017, db='pholcus', tube='default', timeout=30, items=None, unfinished_tasks=None, init=True, weight=[]):
        self.mc = pymongo.MongoClient(host='localhost', port=port)[db]
        self.tube = tube
        self.unfinished_tasks = 0

        if self.tube in Queue.conditions:
            pass
        else:
            Queue.conditions[self.tube] = {'unfinished_tasks': unfinished_tasks or 0, 'event': threading.Event(), 'mutex':threading.Lock(), 'weight':weight}
            Queue.conditions[self.tube]['event'].set()
        if init:
            self.clear()
        if items:
            for item in items:
                self.put(item)

    def funid(self, methodName, methodId=None):
        if methodId is None:
            return self.mc['%s-funid' % self.tube].find_one({'methodName':'%s-%s' % (MACADDRESS, methodName)})['methodId']
        else:
            self.mc['%s-funid' % self.tube].update({'methodName':'%s-%s' % (MACADDRESS, methodName)}, {'$set':{'methodId':methodId, 'methodName':'%s-%s' % (MACADDRESS, methodName)}}, upsert=True)

    def put(self, item):
        priority, methodName, times, args, kwargs, tid = item
        sid = ObjectId()
        txt = pickle.dumps({'priority': priority, 'methodName':methodName, 'times': times, 'args': args, 'kwargs': kwargs, 'tid':tid})
        self.mc[self.tube].insert({'_id':sid, 'priority':priority, 'methodName':methodName, 'status':2, 'times':times, 'deny':[], 'tid':tid, 'txt':txt})
        Queue.conditions[self.tube]['event'].clear()

    def get(self, block=True, timeout=0):
        print self.mc
        item = self.mc.run_command({
            'findAndModify':self.tube,
            'query':{'deny':{'$ne':'localhost'}, 'tid':{'$nin':[], 'status':{'$in':[2, 4]}}},
            'sort':{'priority':1},
            'update':{'$set':{'status':3}},
            'upsert':False
        })
        if item:
            item = item['txt']
            item = pickle.loads(item)
            return (item['priority'], self.funid(item['methodName']), item['methodName'], item['times'], tuple(item['args']), item['kwargs'], item['tid']), str(item['_id'])
        else:
            return None

    def empty(self):
        return self.mc[self.tube].find({'deny':{'$ne':'localhost'}, 'tid':{'$nin':[], 'status':{'$in':[2, 4]}}}).count() == 0

    def copy(self):
        pass

    def task_done(self, item, force=False):
        if item is not None:
            tid, sname, priority, times, args, kwargs, sid = item
            _print('', tid=tid, sid=sid, type='COMPLETED', status=1, sname=sname, priority=priority, times=times, args='(%s)' % ', '.join([str(one) for one in args]), kwargs=json.dumps(kwargs, ensure_ascii=False), txt=None)
            self.mc[self.tube].update({'_id':ObjectId(sid)}, {'$set':{'status':1}})
        if self.empty() or force:
            Queue.conditions[self.tube]['event'].set()

    def join(self):
        Queue.conditions[self.tube]['event'].wait()

    def clear(self):
        self.mc[self.tube].remove({})

    def total(self):
        total = self.mc[self.tube].find({'deny':{'$ne':'localhost'}, 'tid':{'$nin':[], 'status':{'$in':[2, 4]}}}).count()
        return total

    def abandon(self, sid):
        self.mc[self.tube].update({'_id':ObjectId(sid)}, {'$set':{'status':5}})

    def traversal(self, skip=0, limit=10):
        result = list(self.mc[self.tube].find({'deny':{'$ne':'localhost'}, 'tid':{'$nin':[], 'status':{'$in':[2, 4]}}}, skip=skip, limit=limit))
        return result

    def __repr__(self):
        return "<" + str(self.__class__).replace(" ", "").replace("'", "").split('.')[-1]

    def collect(self):
        if self.tube in Queue.conditions:
            del Queue.conditions[self.tube]

    def __del__(self):
        del self.mc


if __name__ == '__main__':
    pass

