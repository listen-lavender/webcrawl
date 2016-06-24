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
from . import MyLocal
MTID = threading._get_ident()  # id of main thread
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
SPACE = 100
RETRY = 0
TIMELIMIT = 0

_continuous = True

def callpath(fun):
    return '.'.join([str(fun.clspath), fun.__name__])


def initflow(which):
    def wrap(fun):
        fun.label = which

        @functools.wraps(fun)
        def wrapped(*args, **kwargs):
            return fun(*args, **kwargs)
        return wrapped
    return wrap


def index(key):
    def wrap(fun):
        fun.index = key

        @functools.wraps(fun)
        def wrapped(*args, **kwargs):
            return fun(*args, **kwargs)
        return wrapped
    return wrap


def unique(keys):
    def wrap(fun):
        fun.unique = keys

        @functools.wraps(fun)
        def wrapped(*args, **kwargs):
            return fun(*args, **kwargs)
        return wrapped
    return wrap


def store(db, way, update=None, method=None, priority=0, space=1):
    def wrap(fun):
        if way is not None:
            fun.store = functools.partial(db(way), update=update, method=method)
            fun.store.__name__ = 'store' + way.im_self.__name__
            fun.store.retry = RETRY
            fun.store.timelimit = TIMELIMIT
            fun.store.priority = priority
            fun.store.space = space
            fun.store.succ = 0
            fun.store.fail = 0
            fun.store.timeout = 0

        @functools.wraps(fun)
        def wrapped(*args, **kwargs):
            return fun(*args, **kwargs)
        return wrapped
    return wrap


def hashweb():
    def wrap(fun):
        @functools.wraps(fun)
        def wrapped(*args, **kwargs):
            return fun(*args, **kwargs)
        return wrapped
    return wrap

def retry(num=1):
    def wrap(fun):
        fun.retry = num

        @functools.wraps(fun)
        def wrapped(*args, **kwargs):
            return fun(*args, **kwargs)
        return wrapped
    return wrap


def next(method, *args, **kwargs):
    def wrap(fun):
        try:
            method.args = args
            method.kwargs = kwargs
            fun.next = weakref.proxy(method)
        except:
            method.__func__.args = args
            # method.__func__.args = tuple((str(fun).split('at')[0].split('function')[-1].replace(' ', '') + ',' + ','.join(args)).split(','))
            method.__func__.kwargs = kwargs
            fun.next = method

        @functools.wraps(fun)
        def wrapped(*args, **kwargs):
            return fun(*args, **kwargs)
        return wrapped
    return wrap


def dispatch(flag=False):
    def wrap(fun):
        fun.dispatch = flag

        @functools.wraps(fun)
        def wrapped(*args, **kwargs):
            return fun(*args, **kwargs)
        return wrapped
    return wrap


def timelimit(seconds=TIMELIMIT):
    def wrap(fun):
        fun.timelimit = seconds

        @functools.wraps(fun)
        def wrapped(*args, **kwargs):
            return fun(*args, **kwargs)
        return wrapped
    return wrap


def priority(level=0):
    def wrap(fun):
        fun.priority = level

        @functools.wraps(fun)
        def wrapped(*args, **kwargs):
            return fun(*args, **kwargs)
        return wrapped
    return wrap


def switch(space=SPACE):
    def wrap(fun):
        fun.space = 100

        @functools.wraps(fun)
        def wrapped(*args, **kwargs):
            return fun(*args, **kwargs)
        return wrapped
    return wrap


class Nevertimeout(object):

    def __init__(self):
        pass

    def cancel(self):
        pass

def generateid(tid, method, args, kwargs, times):
    ssid = ''
    for key in getattr(method, 'unique', []):
        if type(key) == int:
            ssid += str(args[key])
        else:
            ssid += str(kwargs[key])
    if ssid:
        ssid = hashlib.md5('%s%s%s' % (str(tid), ssid, str(times))).hexdigest()
    else:
        # ssid = ObjectId()
        # threadid = threading._get_ident()
        # ssid = hashlib.md5('%s%s%.6f' % (str(ssid), str(threadid), time.time())).hexdigest()
        ssid = str(ObjectId())
    return ssid

def handleIndex(workqueue, result, method, args, kwargs, priority, name, times, tid, version):
    index = result.next()
    if index and times == 0:
        if type(method.index) == int:
            indexargs = list(args)
            indexargs[method.index] = index
            indexargs = tuple(indexargs)
            indexkwargs = dict(kwargs, **{})
        elif type(method.index) == str:
            indexargs = tuple(list(args))
            indexkwargs = dict(
                kwargs, **{method.index: index})
        else:
            raise "Incorrect arguments."
        ssid = generateid(tid, method, args, kwargs, times)
        workqueue.put((priority, name, 0, indexargs, indexkwargs, tid, ssid, version))


def handleNextStore(workqueue, retvar, method, tid, version, hasnext=False, hasstore=False, serialno=0):
    if retvar is None:
        pass
    elif type(retvar) == dict:
        if hasnext:
            ssid = generateid(tid, method.next, (), retvar, 0)
            priority = method.next.priority * method.next.space + serialno
            workqueue.put((priority, callpath(method.next), 0, (), retvar, tid, ssid, version))
        if hasstore:
            ssid = generateid(tid, method.store, (), retvar, 0)
            priority = method.store.priority * method.store.space + serialno
            workqueue.put((priority, callpath(method.store), 0, (), {'obj':retvar}, tid, ssid, version))
    elif type(retvar) == tuple:
        if hasnext:
            ssid = generateid(tid, method.next, retvar, {}, 0)
            priority = method.next.priority * method.next.space + serialno
            workqueue.put((priority, callpath(method.next), 0, retvar, {}, tid, ssid, version))
        if hasstore:
            ssid = generateid(tid, method.store, retvar, {}, 0)
            priority = method.store.priority * method.store.space + serialno
            workqueue.put((priority, callpath(method.store), 0, (retvar[0],), {}, tid, ssid, version))
    else:
        if hasnext:
            ssid = generateid(tid, method.next, (retvar,), {}, 0)
            priority = method.next.priority * method.next.space + serialno
            workqueue.put((priority, callpath(method.next), 0, (retvar,), {}, tid, ssid, version))
        if hasstore:
            ssid = generateid(tid, method.store, (retvar,), {}, 0)
            priority = method.store.priority * method.store.space + serialno
            workqueue.put((priority, callpath(method.store), 0, (retvar,), {}, tid, ssid, version))
        # raise "Incorrect result for next function."


def handleExcept(workqueue, method, args, kwargs, priority, name, times, tid, ssid, version, create_time, count='FAIL'):
    if times < method.retry:
        times = times + 1
        ssid = generateid(tid, method, args, kwargs, times)
        workqueue.put((priority, name, times, args, kwargs, tid, ssid, version))
        
    setattr(method, count, getattr(method, count.lower())+1)
    t, v, b = sys.exc_info()
    err_messages = traceback.format_exception(t, v, b)
    txt = ','.join(err_messages)
    workqueue.task_skip((tid, ssid, count, txt, create_time))


def geventwork(workqueue):
    while _continuous:
        if workqueue.empty():
            sleep(0.1)
        else:
            timer = Nevertimeout()
            item = workqueue.get(timeout=10)
            if item is None:
                continue
            priority, mid, name, times, args, kwargs, tid, ssid, version= item
            method = ctypes.cast(mid, ctypes.py_object).value
            try:
                if method.timelimit > 0:
                    timer = Timeout(method.timelimit, TimeoutError)
                    timer.start()
                create_time = time.time()
                result = method(*args, **kwargs)
                if result is None:
                    method.succ = method.succ + 1
                elif isinstance(result, types.GeneratorType):
                    hasattr(method, 'index') and handleIndex(
                        workqueue, result, method, args, kwargs, priority, name, times, tid, version)
                    serialno = 0
                    for retvar in result:
                        handleNextStore(
                            workqueue, retvar, method, tid, version, hasattr(method, 'next'), hasattr(method, 'store'), serialno)
                        serialno = serialno + 1
                    method.succ = method.succ + 1
                else:
                    handleNextStore(
                        workqueue, result, method, tid, version, hasattr(method, 'next'), hasattr(method, 'store'))
                    method.succ = method.succ + 1
            except TimeoutError:
                handleExcept(
                    workqueue, method, args, kwargs, priority, name, times, tid, ssid, version, create_time, 'TIMEOUT')
            except:
                handleExcept(
                    workqueue, method, args, kwargs, priority, name, times, tid, ssid, version, create_time, 'FAIL')
            else:
                workqueue.task_done((tid, ssid, 'SUCC', None, create_time))
            finally:
                timer.cancel()
                del timer


class Foreverworker(threading.Thread):

    """
        永久执行
    """

    def __init__(self, workqueue):
        """
            初始化多线程运行的方法和方法参数
            @param workqueue: 方法
        """
        super(Foreverworker, self).__init__()
        self.__workqueue = workqueue

    def run(self):
        """
            多线程执行
        """
        geventwork(self.__workqueue)

    def __del__(self):
        del self.__workqueue


class Workflows(object):

    """
        任务流
    """

    def __init__(self, worknum, queuetype, worktype, tid='', settings={}):
        if worktype == 'COROUTINE':
            monkey.patch_all(Event=True)
            gid = threading._get_ident()
            thread = threading._active.get(MTID)
            if thread:
                threading._active[gid] = thread
        self.__worknum = worknum
        self.__queuetype = queuetype
        self.__worktype = worktype
        self.__flowcount = {'inner': set(), 'outer': set()}
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
                QCLS = LocalQueue()
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

        global sleep
        if self.__worktype == 'COROUTINE':
            from gevent import sleep
            for k in range(self.__worknum):
                if self.__queuetype == 'P':
                    queue = self.queue
                else:
                    queue = QCLS(**self.settings)
                worker = functools.partial(geventwork, queue)
                self.workers.append(worker)
        else:
            from time import sleep
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

    def terminator(self, flow):
        return self.__flows[flow]['terminator']

    def extractFlow(self):
        def imitate(p, b):
            if not hasattr(b, '__name__'):
                b.__name__ = str(p).split(' at ')[0].split(' of ')[0].split(
                    '<function ')[-1].split('.')[-1].replace(' ', '').replace('>', '')
            b.succ = 0
            b.fail = 0
            b.timeout = 0
            hasattr(p, 'index') and setattr(b, 'index', p.index)
            hasattr(p, 'unique') and setattr(b, 'unique', p.unique)
            setattr(b, 'space', (hasattr(b, 'space') and getattr(p, 'space')) or 1)
            setattr(b, 'clspath', str(self))
            hasattr(p, 'store') and setattr(b, 'store', p.store)
            hasattr(p, 'store') and setattr(b.store, 'clspath', str(self))
            setattr(b, 'retry', (hasattr(b, 'retry') and getattr(p, 'retry')) or RETRY)
            setattr(b, 'timelimit', (hasattr(b, 'timelimit') and getattr(p, 'timelimit')) or TIMELIMIT)
            setattr(b, 'priority', (hasattr(b, 'priority') and getattr(p, 'priority')) or None)
        if self.__flowcount['inner']:
            print "Inner workflow can be set once and has been set."
        else:
            for it in dir(self):
                it = getattr(self, it)
                if hasattr(it, 'label'):
                    self.__flows[it.label] = {'tinder': it, 'terminator': it, 'weight':{'num':0, 'levels':[]}}
            for label, flow in self.__flows.items():
                flow['hasprior'] = True
                flow['steps'] = 1
                p = flow['tinder']
                b = functools.partial(p)
                imitate(p, b)
                flow['hasprior'] = flow['hasprior'] and (
                    b.priority is not None)
                self.__flows[label]['weight']['levels'].append(b.priority)
                flow['tinder'] = b
                self.__flowcount['inner'].add(p.label)
                while hasattr(p, 'next') and hasattr(p.next, 'args') and hasattr(p.next, 'kwargs'):
                    p = p.next
                    flow['steps'] = flow['steps'] + 1
                    if hasattr(p, 'dispatch') and p.dispatch:
                        b.next = p(self, *p.args, **p.kwargs)
                    else:
                        b.next = functools.partial(
                            p, self, *p.args, **p.kwargs)
                    b = b.next
                    imitate(p, b)
                    flow['hasprior'] = flow['hasprior'] and (
                        b.priority is not None)
                    self.__flows[label]['weight']['levels'].append(b.priority)
                    flow['terminator'] = b
            for label, flow in self.__flows.items():
                if not flow['hasprior']:
                    self.__flows[label]['weight']['levels'] = []
                    it = flow['tinder']
                    num = 0
                    it.priority = flow['steps'] - num
                    self.__flows[label]['weight']['levels'].append(it.priority)
                    while hasattr(it, 'next'):
                        it = it.next
                        num = num + 1
                        it.priority = flow['steps'] - num
                        self.__flows[label]['weight']['levels'].append(it.priority)
                    flow['hasprior'] = True
                self.__flows[label]['weight']['levels'].append(0)
            print "Inner workflow is set."

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
                self.queue.put((it.priority * it.space, callpath(it), 0, args, kwargs, str(self.tid), ssid, version))
                self.queue.funid(callpath(it), id(it))
                if hasattr(it, 'store'):
                    self.queue.funid(callpath(it.store), id(it.store))
                while hasattr(it, 'next'):
                    it = it.next
                    self.queue.funid(callpath(it), id(it))
                    if hasattr(it, 'store'):
                        self.queue.funid(callpath(it.store), id(it.store))
                for worker in self.workers:
                    if self.__worktype == 'COROUTINE':
                        gevent.spawn(worker)
                    else:
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
            if self.__worktype == 'COROUTINE':
                gevent.spawn(worker)
            else:
                worker.setDaemon(True)
                worker.start()

    def task(self, section, tid, version, *args, **kwargs):
        it = section
        self.queue.funid(callpath(it), id(it))
        if hasattr(it, 'store'):
            self.queue.funid(callpath(it.store), id(it.store))
        while hasattr(it, 'next'):
            it = it.next
            self.queue.funid(callpath(it), id(it))
            if hasattr(it, 'store'):
                self.queue.funid(callpath(it.store), id(it.store))
        ssid = generateid(tid, it, args, kwargs, 0)
        self.queue.put((section.priority * section.space, callpath(section), 0, args, kwargs, str(tid), ssid, version))

    def __str__(self):
        desc = object.__str__(self)
        return desc.replace("<", "").split(" ")[0]

    def __del__(self):
        if self.queue is not None:
            self.queue.collect()
        del self.queue
        del self.workers
        if threading._active[MTID]:
            del threading._active[MTID]

if __name__ == '__main__':
    pass
