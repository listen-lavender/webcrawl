#!/usr/bin/env python
# coding=utf-8

import heapq
import pymongo
import threading
import cPickle as pickle
from bson import ObjectId

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

DESCRIBE = {-2:'ABANDONED', -1:'ERROR', 0:'COMPLETED', 1:'RETRY', 2:'WAIT', 3:'RUNNING'}

class Queue(object):
    conditions = {}

    def __init__(self, host='localhost', port=27017, db='pholcus', tube='', timeout=30, items=None, unfinished_tasks=None, init=True):
        self.mc = pymongo.MongoClient(host=host, port=port)[db]
        self.tube = 'task%s' % tube
        self.unfinished_tasks = 0

        if self.tube in Queue.conditions:
            pass
        else:
            Queue.conditions[self.tube] = {'event': threading.Event()}
            Queue.conditions[self.tube]['event'].set()
        if init:
            self.clear()
        if items:
            for item in items:
                self.put(item)

    def funid(self, methodName, methodId=None):
        if methodId is None:
            return self.mc['%s_funid' % self.tube].find_one({'methodName':fid(methodName)})['methodId']
        else:
            self.mc['%s_funid' % self.tube].update({'methodName':fid(methodName)}, {'$set':{'methodId':methodId, 'methodName':fid(methodName)}}, upsert=True)

    def put(self, item):
        priority, methodName, times, args, kwargs, tid, sid, version = item
        sid = sid or str(ObjectId())
        txt = pickle.dumps({'priority': priority, 'methodName':methodName, 'times': times, 'args': args, 'kwargs': kwargs, 'tid':tid})
        try:
            status = 2 if times == 0 else 1
            self.mc[self.tube].insert({'_id':sid, 'priority':priority, 'methodName':methodName, 'status':status, 'times':times, 'deny':[], 'tid':tid, 'txt':txt, 'version':version}, continue_on_error=True)
            Queue.conditions[self.tube]['event'].clear()
        except:
            pass

    def get(self, block=True, timeout=0):
        item = self.mc[self.tube].find_one_and_update({'deny':{'$ne':'localhost'}, 'tid':{'$nin':[]}, 'status':{'$in':[2, 1]}}, {'$set':{'status':3}}, sort=[('priority', 1)])
        if item:
            _id = item['_id']
            version = item['version']
            item = item['txt'].encode('utf-8')
            item = pickle.loads(item)
            return item['priority'], self.funid(item['methodName']), item['methodName'], item['times'], tuple(item['args']), item['kwargs'], item['tid'], _id, version
        else:
            return None

    def empty(self):
        return self.mc[self.tube].find({'deny':{'$ne':'localhost'}, 'tid':{'$nin':[]}, 'status':{'$in':[2, 1]}}).count() == 0

    def copy(self):
        pass

    def task_done(self, item, force=False):
        if item is not None:
            tid, sid, version, status, priority, times, args, kwargs, txt, create_time = item
            elapse = time.time() - create_time
            create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(create_time))
            _print('', tid=tid, sid=sid, 
                version=version, status=status, 
                elapse=elapse, priority=priority, 
                times=times, args='(%s)' % ', '.join([str(one) for one in args]), 
                kwargs=json.dumps(kwargs, ensure_ascii=False), txt=txt)
            self.mc[self.tube].update({'_id':sid}, {'$set':{'status':0}})
        if self.empty() or force:
            Queue.conditions[self.tube]['event'].set()

    def task_skip(self, item):
        if item is not None:
            tid, sid, version, status, priority, times, args, kwargs, txt, create_time = item
            elapse = time.time() - create_time
            create_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(create_time))
            _print('', tid=tid, sid=sid, 
                version=version, status=status, 
                elapse=elapse, priority=priority, 
                times=times, args='(%s)' % ', '.join([str(one) for one in args]), 
                kwargs=json.dumps(kwargs, ensure_ascii=False), txt=txt)
            self.mc[self.tube].update({'_id':sid}, {'$set':{'status':-1}})
        if self.empty():
            Queue.conditions[self.tube]['event'].set()

    def join(self):
        Queue.conditions[self.tube]['event'].wait()

    def clear(self):
        self.mc[self.tube].remove({})
        self.mc['%s_funid' % self.tube].remove({})

    def total(self):
        total = self.mc[self.tube].find({'deny':{'$ne':'localhost'}, 'tid':{'$nin':[], 'status':{'$in':[3, 2, 1]}}}).count()
        return total

    def abandon(self, sid):
        self.mc[self.tube].update({'_id':sid}, {'$set':{'status':-2}})

    def query_do(self, skip=0, limit=10):
        result = list(self.mc[self.tube].find({'deny':{'$ne':'localhost'}, 'tid':{'$nin':[], 'status':{'$in':[2, 1]}}}, skip=skip, limit=limit))
        return result

    def query_succeed(self, skip=0, limit=10):
        result = list(self.mc[self.tube].find({'deny':{'$ne':'localhost'}, 'tid':{'$nin':[], 'status':0}}, skip=skip, limit=limit))
        return result

    def query_fail(self, skip=0, limit=10):
        result = list(self.mc[self.tube].find({'deny':{'$ne':'localhost'}, 'tid':{'$nin':[], 'status':-1}}, skip=skip, limit=limit))
        return result

    def query_rubbish(self, skip=0, limit=10):
        result = list(self.mc[self.tube].find({'deny':{'$ne':'localhost'}, 'tid':{'$nin':[], 'status':-2}}, skip=skip, limit=limit))
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

