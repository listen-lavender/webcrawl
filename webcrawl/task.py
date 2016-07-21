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
from . import DEFAULT, SPACE, RETRY, TIMELIMIT, CONTINUOUS, Enum
MTID = threading._get_ident()  # id of main thread
from time import sleep
from gevent import monkey, Timeout
from bson import ObjectId

from queue.mongo import Queue as MongoQueue
from queue.redis import Queue as RedisQueue
from queue.beanstalkd import Queue as BeanstalkdQueue
from queue.local import Queue as LocalQueue
from exception import TimeoutError

CFG = Enum(L='Local', B='Beanstalkd', R='Redis', M='Mongo')

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
    if hasattr(method, 'store'):
        method.store.name = '%s.%s' % (cls, method.store.__name__)
    method.next = []
    method.succ = 0
    method.fail = 0
    method.timeout = 0
    setattr(method, 'unique', getattr(method, 'unique', []))
    setattr(method, 'params', getattr(method, 'params', []))
    setattr(method, 'space', getattr(method, 'space', SPACE))
    setattr(method, 'retry', getattr(method, 'retry', RETRY))
    setattr(method, 'timelimit', getattr(method, 'timelimit', TIMELIMIT))

class Next(object):
    footprint = {}

    def __init__(self, *next):
        self.next = list(next)
        for index, item in enumerate(self.next):
            if callable(item):
                self.next[index] = item.__name__
        self.attach = {}
        self.circle = {
            'parent_node': None,
            'child_nodes': []
        }

    def __call__(self, curr):
        self.curr = curr
        self.attach = {}
        self.rank = 1
        self.__name__ = curr.__name__
        if self.__name__ in self.next:
            self.next.remove(self.__name__)
        return self

    def update_rank(self, cls, obj, base=0, path=[]):
        if self.__name__ in path:
            path = path[path.index(self.__name__)+1:]
            self.circle['child_nodes'] = path
        else:
            path.append(self.__name__)

            self.rank = self.rank + base
            obj.reversal = max(self.rank, obj.reversal)

        for attr in self.circle['child_nodes']:
            method = cls.__dict__[attr]
            method.circle['parent_node'] = self.__name__
            method.rank = 0
            method.update_rank(cls, obj, base=self.rank, path=[])

        for attr in self.next:
            method = cls.__dict__[attr]
            child_nodes = getattr(method, 'circle', {'child_nodes':[]})['child_nodes']
            done = len(child_nodes) > 0
            done = done and self.__name__ in child_nodes
            done = done or getattr(method, 'circle', {'parent_node':None})['parent_node'] is not None
            if done:
                continue

            next_path = []
            next_path.extend(path)
            if hasattr(method, 'update_rank'):
                method.update_rank(cls, obj, base=self.rank, path=next_path)
            else:
                method.rank = max(1 + self.rank, getattr(method, 'rank', 1))
                obj.reversal = max(method.rank, obj.reversal)

    def __get__(self, obj, cls):
        name = '%s.%s' % (str(obj), self.__name__)
        curr = self.footprint.get(name)
        if not curr:
            curr = functools.partial(self.curr, obj)
            clone(self.curr, curr, self.attach)
            curr.name = name
            assure(curr, str(obj))

            self.footprint[name] = curr

        curr.rank = self.rank
        if curr.next:
            return curr

        if hasattr(curr, 'label'):
            self.update_rank(cls, obj, path=[])

        for attr in self.next:
            name = '%s.%s' % (str(obj), attr)
            next = self.footprint.get(name)

            if not next:
                method = cls.__dict__[attr]
                if not isinstance(method, Next):
                    method.name = name
                    method.rank = getattr(method, 'rank', 1)
                    method = Next()(method)
                next = functools.partial(method.curr, obj)
                clone(method.curr, next, method.attach)
                next.name = name
                assure(next, str(obj))
                # else:
                #     next = getattr(obj, attr)
                #     next.__func__.name = name
                #     next.__func__.rank = getattr(method, 'rank', 1)
                #     assure(next, str(obj), instancemethod=True)
                #     print next, next.__name__, next.succ

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

def store(db, way, update=None, method=None, priority=1, space=1, unique=[], target=None):
    def deco(fun):
        store = functools.partial(db(way), update=update, method=method)
        try:
            store.__name__ = 'store_%s' % way.im_self.__name__.lower()
        except:
            store.__name__ = 'store_%s' % way.im_class.__name__.lower()
        store.retry = RETRY
        store.timelimit = TIMELIMIT
        store.priority = priority
        store.space = space
        store.succ = 0
        store.fail = 0
        store.timeout = 0
        store.unique = unique
        store.target = target

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

def pack_current_step(index, index_type, index_val, flow, priority, name, args, kwargs, tid, version, unique=[]):
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
    ssid = generateid(tid, indexargs, indexkwargs, 0, unique)
    return flow, priority, name, 0, indexargs, indexkwargs, tid, ssid, version

def pack_next_step(retval, flow, priority, name, tid, version, serialno=0, params=[], unique=[]):
    args = []
    kwargs = {}
    priority = priority + serialno
    if type(retval) == dict:
        for key in params:
            key in retval and args.append(retval[key])
        if not kwargs and not params:
            kwargs = retval
    elif type(retval) == tuple:
        for key in params:
            key < len(retval) and args.append(retval[key])
        if not args and not params:
            args = retval
    else:
        args = (retval, )
    args = tuple(args)
    ssid = generateid(tid, args, kwargs, 0, unique)
    if args or kwargs:
        return flow, priority, name, 0, args, kwargs, tid, ssid, version

def pack_store(retval, flow, priority, name, tid, version, serialno=0, target=None, unique=[]):
    args = []
    kwargs = {}
    priority = priority + serialno
    if target is None:
        args = (retval, )
    else:
        try:
            args = (retval.pop(target), )
        except:
            args = ()
    ssid = generateid(tid, args, kwargs, 0, unique)
    if args:
        return flow, priority, name, 0, args, kwargs, tid, ssid, version

def pack_except(times, flow, priority, name, args, kwargs, tid, version, unique=[]):
    times = times + 1
    ssid = generateid(tid, args, kwargs, times, unique)
    return flow, priority, name, times, args, kwargs, tid, ssid, version

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
        flow, priority, mid, name, times, args, kwargs, tid, ssid, version = item
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
                retval = result.next()
                retval and times == 0 and workqueue.put(pack_current_step(index, type(index), retval, flow, priority, name, args, kwargs, tid, version, unique))
            serialno = 0
            for retval in result:
                if retval is None:
                    continue
                if store:
                    store_priority = store.priority * store.space
                    store_serialno = serialno if store.space > 1 else 0
                    package = pack_store(retval, flow, store_priority, store.name, tid, version, store_serialno, store.target, store.unique)
                    if package is not None:
                        workqueue.put(package)
                for next_method in next:
                    next_priority = getattr(next_method, '%s_prior' % flow) * next_method.space
                    next_serialno = serialno if next_method.space > 1 else 0
                    package = pack_next_step(retval, flow, next_priority, next_method.name, tid, version, next_serialno, next_method.params, next_method.unique)
                    if package is not None:
                        workqueue.put(package)
                serialno = serialno + 1
            method.succ = method.succ + 1
        except TimeoutError:
            workqueue.put(pack_except(times, flow, priority, name, args, kwargs, tid, version, unique))
            method.timeout = method.timeout + 1
            txt = format_except()
            workqueue.task_skip((tid, ssid, 'TIMEOUT', txt, create_time))
        except:
            workqueue.put(pack_except(times, flow, priority, name, args, kwargs, tid, version, unique))
            method.fail = method.fail + 1
            txt = format_except()
            workqueue.task_skip((tid, ssid, 'FAIL', txt, create_time))
        else:
            workqueue.task_done((tid, ssid, 'SUCC', None, create_time))
        finally:
            timer.cancel()
            del timer


class Onceworker(threading.Thread):

    def __init__(self, fun, args, kwargs, callback=None):
        super(Onceworker, self).__init__()
        self.__fun = fun
        self.__args = args
        self.__kwargs = kwargs
        self.__callback = callback

    def run(self):
        self.__fun(*args, **kwargs)
        if self.__callback:
            self.__callback()


class Foreverworker(threading.Thread):

    def __init__(self, workqueue):
        super(Foreverworker, self).__init__()
        self.__workqueue = workqueue

    def run(self):
        geventwork(self.__workqueue)

    def __del__(self):
        del self.__workqueue


class Workflows(object):

    def __init__(self, worknum=3, queuetype=CFG.L, tid='', settings={}):
        self.__worknum = worknum
        self.__queuetype = queuetype
        self.__flows = {}
        self.__weight = []
        if not hasattr(self, 'clsname'):
            self.clsname = str(self.__class__).split(".")[-1].replace("'>", "")

        self.workers = []
        self.tid = tid
        self.settings = settings
        self.reversal = 0

        if self.__queuetype == CFG.L:
            self.settings = {}
            QCLS = LocalQueue
            # self.__queue = LocalQueue()()
        elif self.__queuetype == CFG.B:
            QCLS = BeanstalkdQueue
            # self.__queue = BeanstalkdQueue(**dict(DataQueue.beanstalkd, **tube))
        elif self.__queuetype == CFG.R:
            QCLS = RedisQueue
            # self.__queue = RedisQueue(weight=weight, **dict(DataQueue.redis, **tube))
        elif self.__queuetype == CFG.M:
            QCLS = MongoQueue
            # self.__queue = MongoQueue(**dict(DataQueue.mongo, **tube))
        else:
            raise Exception('Error queue type.')
        self.__queue = QCLS(**self.settings)
        self.extract()

    def extract(self):

        self.__flows = {}
        for attr in dir(self):
            if attr.startswith('__') or attr.startswith('_'):
                continue
            method = getattr(self, attr)
            if hasattr(method, 'label'):
                if not hasattr(method, 'next'):
                    name = '%s.%s' % (str(self), attr)
                    method.__func__.rank = 1
                    method = Next()(method.__func__)
                    function = functools.partial(method.curr, self)
                    function.name = name
                    function.rank = getattr(method, 'rank', 1)
                    clone(method.curr, function, method.attach)
                    assure(function, str(self))
                    method = function
                    Next.footprint[name] = function
                self.__flows[method.label] = method

        for label in self.__flows:
            notuserank = True
            for step in self.traversal(self.tinder(label), []):
                if step is None:
                    continue
                self.__queue.funid(step.name, id(step))
                if hasattr(step, 'store'):
                    self.__queue.funid(step.store.name, id(step.store))
                notuserank = notuserank and hasattr(step, 'priority')

            for step in self.traversal(self.tinder(label), []):
                if step is None:
                    continue
                if notuserank:
                    priority = step.priority
                else:
                    priority = self.reversal + 2 - getattr(self, step.__name__).rank
                setattr(step, '%s_prior' % label, priority)
                self.__weight.append(priority)
                if hasattr(step, 'store'):
                    self.__weight.append(step.store.priority)

        self.__weight = list(set(self.__weight))
        self.__weight.sort()
        
    def prepare(self):
        QCLS = self.__queue.__class__
        self.settings['init'] = False
        self.settings['weight'] = self.__weight
        for k in range(self.__worknum):
            if self.__queuetype == CFG.L:
                queue = self.__queue
            else:
                queue = QCLS(**self.settings)
            worker = Foreverworker(queue)
            self.workers.append(worker)

    def tinder(self, flow):
        return self.__flows[flow]

    def fire(self, flow, step, version, *args, **kwargs):
        self.prepare()
        priority = getattr(step, '%s_prior' % flow)
        ssid = generateid(self.tid, args, kwargs, 0, step.unique)
        self.__queue.put((flow, priority * step.space, step.name, 0, args, kwargs, str(self.tid), ssid, version))
        for worker in self.workers:
            worker.setDaemon(True)
            worker.start()

    def exit(self):
        self.__queue.task_done(None, force=True)

    def wait(self):
        self.__queue.join()

    def start(self):
        self.prepare()
        for worker in self.workers:
            worker.setDaemon(True)
            worker.start()

    def record(self, step):
        for step in self.traversal(step, []):
            if step is None:
                continue
            self.__queue.funid(step.name, id(step))
            if hasattr(step, 'store'):
                self.__queue.funid(step.store.name, id(step.store))

    def select(self, flow, section=None):
        if section is None:
            return self.tinder(flow)
        if hasattr(self, section):
            return getattr(self, section)
        return Next.footprint[section]

    def task(self, flow, step, tid, version, *args, **kwargs):
        ssid = generateid(tid, args, kwargs, 0, step.unique)
        priority = getattr(step, '%s_prior' % flow)
        self.__queue.put((flow, priority * step.space, step.name, 0, args, kwargs, str(tid), ssid, version))

    def traversal(self, step, footprint=[]):
        if step.__name__ in footprint:
            yield None

        else:
            yield step
            # print step.name, getattr(step, '%s_prior' % flow)
            # if hasattr(step, 'store'):
            #     print step.store.name, step.store.priority

            footprint.append(step.__name__)

            if hasattr(step, 'next'):
                for next_method in step.next:
                    for next_step in self.traversal(next_method, footprint):
                        yield next_step

    def __str__(self):
        desc = object.__str__(self)
        return desc.replace("<", "").replace("__main__.", "").split(" ")[0]

    def __del__(self):
        if self.__queue is not None:
            self.__queue.collect()
        del self.__queue
        del self.workers

if __name__ == '__main__':
    pass
