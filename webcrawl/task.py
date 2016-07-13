#!/usr/bin/env python
# coding=utf-8
import json
import hashlib
import threading
import types
import copy
import sys
import traceback
import time
import weakref
import gevent
import functools
import ctypes
from . import DEFAULT, SPACE, RETRY, TIMELIMIT, CONTINUOUS
MTID = threading._get_ident()  # id of main thread
from time import sleep
from gevent import monkey, Timeout
from bson import ObjectId

from queue.mongo import Queue as MongoQueue
from queue.redis import Queue as RedisQueue
from queue.beanstalkd import Queue as BeanstalkdQueue
from queue.local import Queue as LocalQueue
from exception import TimeoutError


def patch_thread(threading=True, _threading_local=True, Queue=True, Event=False):
    """Replace the standard :mod:`thread` module to make it greenlet-based.
    If *threading* is true (the default), also patch ``threading``.
    If *_threading_local* is true (the default), also patch ``_threading_local.local``.
    """
    monkey.patch_module('thread')
    if threading:
        monkey.patch_module('threading')
        threading = __import__('threading')
        if Event:
            from gevent.event import Event
            threading.Event = Event
        if Queue:
            from gevent import queue
            threading.queue = queue
    if _threading_local:
        _threading_local = __import__('_threading_local')
        from gevent.local import local
        _threading_local.local = local

monkey.patch_thread = patch_thread

def clone(src, obj, attach={}):
    for attr in dir(src):
        if attr in DEFAULT:
            continue
        setattr(obj, attr, getattr(src, attr))
    for attr in attach:
        setattr(obj, attr, attach[attr])

def assure(method, cls, instancemethod=False):
    obj = method
    if instancemethod:
        obj = method.__func__
    if hasattr(obj, 'store'):
        obj.store.name = '%s.%s' % (cls, obj.store.__name__)
    obj.next = []
    obj.succ = 0
    obj.fail = 0
    obj.timeout = 0
    obj.next = []
    setattr(obj, 'params', getattr(obj, 'params', []))
    setattr(obj, 'space', getattr(obj, 'space', SPACE))
    setattr(obj, 'retry', getattr(obj, 'retry', RETRY))
    setattr(obj, 'timelimit', getattr(obj, 'timelimit', TIMELIMIT))

class Next(object):
    footprint = {}

    def __init__(self, *next):
        self.next = list(next)
        self.attach = {}

    def __call__(self, curr):
        self.curr = curr
        self.priority = getattr(curr, 'priority', self.attach.get('priority', 1))
        self.__name__ = curr.__name__
        if self.__name__ in self.next:
            self.next.remove(self.__name__)
        return self

    def update_priority(self, cls, base=0):
        self.priority = self.priority + base
        cls.reversal = max(self.priority, cls.reversal)
        for attr in self.next:
            method = cls.__dict__[attr]
            if hasattr(method, 'update_priority'):
                method.update_priority(cls, self.priority)
            else:
                method.priority = max(1 + self.priority, getattr(method, 'priority', 1))
                cls.reversal = max(method.priority, cls.reversal)

    def __get__(self, obj, cls):
        name = '%s.%s' % (str(obj), self.__name__)
        curr = self.footprint.get(name)
        if not curr:
            curr = functools.partial(self.curr, obj)
            clone(self.curr, curr, self.attach)
            curr.name = name
            assure(curr, str(obj))

            self.footprint[name] = curr

        curr.priority = self.priority
        if curr.next:
            return curr

        self.update_priority(cls)

        for attr in self.next:
            name = '%s.%s' % (str(obj), attr)
            next = self.footprint.get(name)

            if not next:
                method = cls.__dict__[attr]
                if isinstance(method, Next):
                    next = functools.partial(method.curr, obj)
                    clone(method.curr, next, method.attach)
                    next.name = name
                    assure(next, str(obj))
                else:
                    next = getattr(obj, attr)
                    next.__func__.name = name
                    next.__func__.priority = method.priority
                    assure(next, str(obj), instancemethod=True)

                self.footprint[name] = next

            curr.next.append(next)
        
        return curr

next = Next

def _deco(fun, key, val):
    if isinstance(fun, Next):
        fun.attach[key] = val
        return fun
    else:
        setattr(fun, key, val)

        @functools.wraps(fun)
        def __deco(*args, **kwargs):
            return fun(*args, **kwargs)
        return __deco

def initflow(which):
    def deco(fun):
        return _deco(fun, 'label', which)
    return deco

def index(key):
    def deco(fun):
        return _deco(fun, 'index', key)
    return deco

def params(*keys):
    def deco(fun):
        return _deco(fun, 'params', keys)
    return deco

def unique(*keys):
    def deco(fun):
        return _deco(fun, 'unique', keys)
    return deco

def retry(num=1):
    def deco(fun):
        return _deco(fun, 'retry', num)
    return deco

def timelimit(seconds=TIMELIMIT):
    def deco(fun):
        return _deco(fun, 'timelimit', seconds)
    return deco

def priority(level=0):
    def deco(fun):
        return _deco(fun, 'priority', level)
    return deco

def switch(space=SPACE):
    def deco(fun):
        return _deco(fun, 'space', space)
    return deco

def store(db, way, update=None, method=None, priority=0, space=1, obj=None):
    def deco(fun):
        store = functools.partial(db(way), update=update, method=method)
        store.__name__ = 'store_%s' % way.im_class.__name__.lower()
        store.retry = RETRY
        store.timelimit = TIMELIMIT
        store.priority = priority
        store.space = space
        store.succ = 0
        store.fail = 0
        store.timeout = 0

        return _deco(fun, 'store', store)
    return deco


class Nevertimeout(object):

    def __init__(self):
        pass

    def cancel(self):
        pass

def generateid(tid, args, kwargs, times, unique=[]):
    ssid = ''
    for key in unique:
        if type(key) == int:
            ssid += str(args[key])
        else:
            ssid += str(kwargs[key])
    if ssid:
        ssid = hashlib.md5('%s%s%s' % (str(tid), ssid, str(times))).hexdigest()
    else:
        ssid = str(ObjectId())
    return ssid

def pack_current_step(index, index_type, index_val, priority, name, args, kwargs, tid, version, unique=[]):
    if index_type == int:
        indexargs = list(args)
        indexargs[index] = index_val
        indexargs = tuple(indexargs)
        indexkwargs = dict(kwargs, **{})
    elif index_type == str:
        indexargs = tuple(list(args))
        indexkwargs = dict(kwargs, **{index: index_val})
    else:
        raise "Incorrect arguments."
    ssid = generateid(tid, args, kwargs, times, unique)
    return priority, name, 0, args, kwargs, tid, ssid, version

def pack_next_step(retvar, priority, name, tid, version, serialno=0, params=[], unique=[]):
    args = []
    kwargs = {}
    priority = priority + serialno
    if type(retvar) == dict:
        kwargs = retvar
    elif type(retvar) == tuple:
        args = retvar
    else:
        args = (retvar, )
    args = tuple(args)
    ssid = generateid(tid, args, kwargs, times, unique)
    return priority, name, 0, args, kwargs, tid, ssid, version

def pack_store(retvar, priority, name, tid, version, serialno=0, unique=[]):
    args = (retvar, )
    kwargs = {}
    priority = priority + serialno
    ssid = generateid(tid, args, kwargs, times, unique)
    return priority, name, 0, args, kwargs, tid, ssid, version

def pack_except(times, priority, name, args, kwargs, tid, version, unique=[]):
    times = times + 1
    ssid = generateid(tid, args, kwargs, times, unique)
    return priority, name, times, args, kwargs, tid, ssid, version

def format_except():    
    t, v, b = sys.exc_info()
    err_messages = traceback.format_exception(t, v, b)
    txt = ','.join(err_messages)
    return txt

def geventwork(workqueue):
    while CONTINUOUS:
        if workqueue.empty():
            sleep(0.1)
            continue
        timer = Nevertimeout()
        item = workqueue.get(timeout=10)
        if item is None:
            continue
        priority, mid, name, times, args, kwargs, tid, ssid, version= item
        method = ctypes.cast(mid, ctypes.py_object).value
        index = getattr(method, 'index', None)
        next = getattr(method, 'next', [])
        store = getattr(method, 'store', None)
        unique = getattr(method, 'unique', [])
        try:
            if method.timelimit > 0:
                timer = Timeout(method.timelimit, TimeoutError)
                timer.start()
            create_time = time.time()
            result = method(*args, **kwargs)
            if result is None:
                method.succ = method.succ + 1
                result = []
            elif isinstance(result, types.GeneratorType):
                pass
            else:
                result = iter([result, result]) if index else iter([result, ])
            if result and index:
                retvar = result.next()
                retvar and times == 0 and workqueue.put(pack_current_step(index, type(index), retvar, priority, name, args, kwargs, tid, version, unique))
            serialno = 0
            for retvar in result:
                if retvar is None:
                    continue
                for next_method in next:
                    next_priority = next_method.priority * next_method.space
                    workqueue.put(pack_next_step(retvar, next_priority, next_method.name, tid, version, serialno, next_method.params, next_method.unique))
                store and workqueue.put(pack_store(retvar, store.priority * store.space, store.name, tid, version, serialno, store.unique))
                serialno = serialno + 1
            method.succ = method.succ + 1
        except TimeoutError:
            workqueue.put(pack_except(times, priority, name, args, kwargs, tid, version, unique))
            method.timeout = method.timeout + 1
            txt = format_except()
            workqueue.task_skip((tid, ssid, 'TIMEOUT', txt, create_time))
        except:
            workqueue.put(pack_except(times, priority, name, args, kwargs, tid, version, unique))
            method.fail = method.fail + 1
            txt = format_except()
            workqueue.task_skip((tid, ssid, 'FAIL', txt, create_time))
        else:
            workqueue.task_done((tid, ssid, 'SUCC', None, create_time))
        finally:
            timer.cancel()
            del timer


class Foreverworker(threading.Thread):

    def __init__(self, workqueue):
        super(Foreverworker, self).__init__()
        self.__workqueue = workqueue

    def run(self):
        geventwork(self.__workqueue)

    def __del__(self):
        del self.__workqueue


class Workflows(object):

    """
        任务流
    """

    def __init__(self, worknum, queuetype, tid='', settings={}):
        self.__worknum = worknum
        self.__queuetype = queuetype
        self.__flows = {}
        if not hasattr(self, 'clsname'):
            self.clsname = str(self.__class__).split(".")[-1].replace("'>", "")

        self.queue = None
        self.workers = []
        self.tid = tid
        self.settings = settings
        
    def prepare(self, flow=None):
        self.workers = []
        weight = []
        tube = {}
        if flow:
            weight = self.weight(flow)
            self.settings['tube'] = str(id(self))

        try:
            if self.__queuetype == 'P':
                self.settings = {}
                QCLS = LocalQueue
                # self.queue = LocalQueue()()
            elif self.__queuetype == 'B':
                QCLS = BeanstalkdQueue
                # self.queue = BeanstalkdQueue(**dict(DataQueue.beanstalkd, **tube))
            elif self.__queuetype == 'R':
                self.settings['weight'] = weight
                QCLS = RedisQueue
                # self.queue = RedisQueue(weight=weight, **dict(DataQueue.redis, **tube))
            elif self.__queuetype == 'M':
                QCLS = MongoQueue
                # self.queue = MongoQueue(**dict(DataQueue.mongo, **tube))
            else:
                raise Exception('Error queue type.')
            self.queue = QCLS(**self.settings)
        except:
            print 'Wrong type of queue, please choose P(local queue), B(beanstalkd queue), R(redis queue), M(mongo queue) start your beanstalkd service.'

        for k in range(self.__worknum):
            if self.__queuetype == 'P':
                queue = self.queue
            else:
                queue = QCLS(**self.settings)
            worker = Foreverworker(queue)
            self.workers.append(worker)

    def tinder(self, flow):
        return self.__flows[flow]['tinder']

    def select(self, flow, step=0):
        section = None
        index = 0
        it = self.__flows.get(flow, {'tinder':None})['tinder']
        if step == index:
            section = it
        while hasattr(it, 'next'):
            it = it.next
            index = index + 1
            if step == index:
                section = it
        return section

    def extract(self):
        self.__flows = {}
        for attr in dir(self):
            if attr.startswith('__'):
                continue
            method = getattr(self, attr)
            if hasattr(method, 'label'):
                self.__flows[method.label] = method

        for attr in dir(self):
            if attr.startswith('__'):
                continue
            method = getattr(self, attr)
            if hasattr(method, 'priority'):
                try:
                    method.priority = self.reversal + 1 - method.priority
                except:
                    method.__func__.priority = self.reversal + 1 - method.priority
            self.queue.funid(method, id(method))
            if hasattr(method, 'store'):
                self.queue.funid(method.store, id(method.store))

    def fire(self, flow, step, version, *args, **kwargs):
        it = self.__flows.get(flow, {'tinder':None})['tinder']
        if it is not None:
            self.prepare(flow)
            try:
                for k in range(step):
                    it = it.next
            except:
                print 'Flow %s has no %d steps.' % (flow, step)
            else:
                ssid = generateid(self.tid, it, args, kwargs, 0)
                self.queue.put((it.priority * it.space, it.name, 0, args, kwargs, str(self.tid), ssid, version))
                for worker in self.workers:
                    worker.setDaemon(True)
                    worker.start()
        else:
            print 'There is no work flow.'

    def exit(self):
        self.queue.task_done(None, force=True)

    def waitComplete(self):
        self.queue.join()

    def weight(self, flow, once=False):
        if once:
            self.__flows[flow]['weight']['num'] = self.__flows[flow]['weight']['num'] + 1
        if once and self.__flows[flow]['weight']['num'] > 1:
            return []
        else:
            return self.__flows[flow]['weight']['levels'][::-1]

    def start(self):
        self.prepare()
        for worker in self.workers:
            worker.setDaemon(True)
            worker.start()

    def task(self, section, tid, version, *args, **kwargs):
        it = section
        self.queue.funid(it.name, id(it))
        if hasattr(it, 'store'):
            self.queue.funid(callpath(it.store), id(it.store))
        while hasattr(it, 'next'):
            it = it.next
            self.queue.funid(it.name, id(it))
            if hasattr(it, 'store'):
                self.queue.funid(callpath(it.store), id(it.store))
        ssid = generateid(tid, it, args, kwargs, 0)
        self.queue.put((section.priority * section.space, callpath(section), 0, args, kwargs, str(tid), ssid, version))

    def __str__(self):
        desc = object.__str__(self)
        return desc.replace("<", "").replace("__main__.", "").split(" ")[0]

    def __del__(self):
        if self.queue is not None:
            self.queue.collect()
        del self.queue
        del self.workers
        if threading._active[MTID]:
            del threading._active[MTID]

if __name__ == '__main__':
    pass
