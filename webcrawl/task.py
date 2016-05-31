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
DataQueue = MyLocal(
        redis={
            'host':'localhost',
            'port':6379,
            'db':0,
            'tube':'pholcus_task',
        },
        beanstalkd={
            'host':'localhost',
            'port':11300,
            'tube':'pholcus_task',
        },
        mongo={
            'host':'localhost',
            'port':27017,
            'tube':'pholcus_task',
        }
    )

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


def store(db, way, update=None, method=None):
    def wrap(fun):
        if way is not None:
            fun.store = functools.partial(db(way), update=update, method=method)
            fun.store.__name__ = 'store' + way.im_self.__name__
            fun.store.retry = RETRY
            fun.store.timelimit = TIMELIMIT
            fun.store.priority = 0
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


def assure(method):
    method.succ = 0
    method.fail = 0
    method.timeout = 0
    not hasattr(method, 'retry') and setattr(method, 'retry', RETRY)
    not hasattr(method, 'timelimit') and setattr(method, 'timelimit', TIMELIMIT)
    not hasattr(method, 'priority') and setattr(method, 'priority', None)

class Nevertimeout(object):

    def __init__(self):
        pass

    def cancel(self):
        pass


def generateId(method, args, kwargs, times):
    sid = ''
    for key in getattr(method, 'unique', []):
        if type(key) == int:
            sid += str(args[key])
        else:
            sid += str(kwargs[key])
    sid = hashlib.md5('%s%s%s%s' % (str(getattr(method, 'aid', '')), str(getattr(method, 'sid', '')), sid, str(times))).hexdigest() if sid else None
    return sid

def handleIndex(workqueue, result, method, args, kwargs, priority, methodName, times, tid, version):
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
        sid = generateId(method, args, kwargs, times)
        workqueue.put((priority, methodName, 0, indexargs, indexkwargs, tid, sid, version))


def handleNextStore(workqueue, retvar, method, tid, version, hasnext=False, hasstore=False):
    if retvar is None:
        pass
    elif type(retvar) == dict:
        if hasnext:
            sid = generateId(method.next, (), retvar, 0)
            workqueue.put((method.next.priority, callpath(method.next), 0, (), retvar, tid, sid, version))
        if hasstore:
            sid = generateId(method.store, (), retvar, 0)
            workqueue.put((method.store.priority, callpath(method.store), 0, (), {'obj':retvar}, tid, sid, version))
    elif type(retvar) == tuple:
        if hasnext:
            sid = generateId(method.next, retvar, {}, 0)
            workqueue.put((method.next.priority, callpath(method.next), 0, retvar, {}, tid, sid, version))
        if hasstore:
            sid = generateId(method.store, retvar, {}, 0)
            workqueue.put((method.store.priority, callpath(method.store), 0, (retvar[0],), {}, tid, sid, version))
    else:
        if hasnext:
            sid = generateId(method.next, (retvar,), {}, 0)
            workqueue.put((method.next.priority, callpath(method.next), 0, (retvar,), {}, tid, sid, version))
        if hasstore:
            sid = generateId(method.store, (retvar,), {}, 0)
            workqueue.put((method.store.priority, callpath(method.store), 0, (retvar,), {}, tid, sid, version))
        # raise "Incorrect result for next function."


def handleExcept(workqueue, method, args, kwargs, priority, methodName, times, tid, sid, version, count='fail'):
    if times < method.retry:
        times = times + 1
        sid = generateId(method, args, kwargs, times)
        workqueue.put((priority, methodName, times, args, kwargs, tid, sid, version))
    else:
        setattr(method, count, getattr(method, count)+1)
        t, v, b = sys.exc_info()
        err_messages = traceback.format_exception(t, v, b)
        txt = ','.join(err_messages)
        item = tid, sid, count.upper(), methodName, priority, times, args, kwargs, txt, version
        workqueue.task_skip(item)


def geventwork(workqueue):
    while _continuous:
        if workqueue.empty():
            sleep(0.1)
        else:
            timer = Nevertimeout()
            item = workqueue.get(timeout=10)
            if item is None:
                continue
            priority, methodId, methodName, times, args, kwargs, tid, sid, version= item
            method = ctypes.cast(methodId, ctypes.py_object).value
            try:
                if method.timelimit > 0:
                    timer = Timeout(method.timelimit, TimeoutError)
                    timer.start()
                result = method(*args, **kwargs)
                if result is None:
                    method.succ = method.succ + 1
                elif isinstance(result, types.GeneratorType):
                    try:
                        hasattr(method, 'index') and handleIndex(
                            workqueue, result, method, args, kwargs, priority, methodName, times, tid, version)
                        for retvar in result:
                            handleNextStore(
                                workqueue, retvar, method, tid, version, hasattr(method, 'next'), hasattr(method, 'store'))
                        method.succ = method.succ + 1
                    except TimeoutError:
                        handleExcept(
                            workqueue, method, args, kwargs, priority, methodName, times, tid, sid, version, 'timeout')
                    except:
                        handleExcept(
                            workqueue, method, args, kwargs, priority, methodName, times, tid, sid, version, 'fail')
                else:
                    handleNextStore(
                        workqueue, result, method, tid, version, hasattr(method, 'next'), hasattr(method, 'store'))
                    method.succ = method.succ + 1
            except TimeoutError:
                handleExcept(
                    workqueue, method, args, kwargs, priority, methodName, times, tid, sid, version, 'timeout')
            except:
                handleExcept(
                    workqueue, method, args, kwargs, priority, methodName, times, tid, sid, version, 'fail')
            finally:
                workqueue.task_done((args, kwargs, priority, methodName, times, tid, sid, version))
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

    def __init__(self, worknum, queuetype, worktype, tid=0, aid=0):
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
        self.aid = aid
        
    def prepare(self, flow=None):
        self.workers = []
        weight = []
        tube = {}
        if flow:
            weight = self.weight(flow)
            tube['tube'] = str(id(self))
        self.queue = LocalQueue()()
        try:
            if self.__queuetype == 'P':
                self.queue = LocalQueue()()
            elif self.__queuetype == 'B':
                self.queue = BeanstalkdQueue(**dict(DataQueue.beanstalkd, **tube))
            elif self.__queuetype == 'R':
                self.queue = RedisQueue(weight=weight, **dict(DataQueue.redis, **tube))
            elif self.__queuetype == 'M':
                self.queue = MongoQueue(**dict(DataQueue.mongo, **tube))
            else:
                raise Exception('Error queue type.')
        except:
            print 'Wrong type of queue, please choose P(local queue), B(beanstalkd queue), R(redis queue), M(mongo queue) start your beanstalkd service.'

        global sleep
        if self.__worktype == 'COROUTINE':
            from gevent import sleep
            for k in range(self.__worknum):
                if self.__queuetype == 'P':
                    worker = functools.partial(geventwork, self.queue)
                elif self.__queuetype == 'B':
                    worker = functools.partial(
                        geventwork, BeanstalkdQueue(**dict(DataQueue.beanstalkd, **tube)))
                elif self.__queuetype == 'R':
                    worker = functools.partial(
                        geventwork, RedisQueue(weight=weight, **dict(DataQueue.redis, **tube)))
                elif self.__queuetype == 'M':
                    worker = functools.partial(
                        geventwork, MongoQueue(**dict(DataQueue.mongo, **tube)))
                else:
                    raise Exception('Error queue type.')
                self.workers.append(worker)
        else:
            from time import sleep
            for k in range(self.__worknum):
                if self.__queuetype == 'P':
                    worker = Foreverworker(self.queue)
                elif self.__queuetype == 'B':
                    worker = Foreverworker(BeanstalkdQueue(**dict(DataQueue.beanstalkd, **tube)))
                elif self.__queuetype == 'R':
                    worker = Foreverworker(RedisQueue(weight=weight, **dict(DataQueue.redis, **tube)))
                elif self.__queuetype == 'M':
                    worker = Foreverworker(MongoQueue(**dict(DataQueue.mongo, **tube)))
                else:
                    raise Exception('Error queue type.')
                self.workers.append(worker)

    def tinder(self, flow):
        return self.__flows[flow]['tinder']

    def select(self, flow, step=0, sid=[]):
        section = None
        index = 0
        it = self.__flows.get(flow, {'tinder':None})['tinder']
        if index < len(sid):
            setattr(it, 'sid', sid[index])
        if step == index:
            section = it
        while hasattr(it, 'next'):
            it = it.next
            index = index + 1
            if index < len(sid):
                setattr(it, 'sid', sid[index])
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
            setattr(b, 'clspath', str(self))
            hasattr(p, 'store') and setattr(b, 'store', p.store)
            hasattr(p, 'store') and setattr(b.store, 'clspath', str(self))
            b.retry = (hasattr(p, 'retry') and p.retry) or RETRY
            b.timelimit = (hasattr(p, 'timelimit') and p.timelimit) or TIMELIMIT
            b.priority = (hasattr(p, 'priority') and p.priority) or None
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
                sid = generateId(it, args, kwargs, 0)
                self.queue.put((it.priority, callpath(it), 0, args, kwargs, str(self.tid), sid, version))
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

    def task(self, weight, section, tid, version, *args, **kwargs):
        self.queue.rank(weight)
        it = section
        self.queue.funid(callpath(it), id(it))
        if hasattr(it, 'store'):
            self.queue.funid(callpath(it.store), id(it.store))
        while hasattr(it, 'next'):
            it = it.next
            self.queue.funid(callpath(it), id(it))
            if hasattr(it, 'store'):
                self.queue.funid(callpath(it.store), id(it.store))
        sid = generateId(it, args, kwargs, 0)
        self.queue.put((section.priority, callpath(section), 0, args, kwargs, str(tid), sid, version))

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
