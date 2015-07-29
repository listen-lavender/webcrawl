#!/usr/bin/python
# coding=utf-8

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
MTID = threading._get_ident()  # id of main thread
from Queue import Queue
from gevent import monkey, Timeout


from queue import BeanstalkdQueue, GPriorjoinQueue, TPriorjoinQueue
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

try:
    from multilog.aboutfile import modulename, modulepath
    from multilog.prettyprint import logprint
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


class MyLocal(threading.local):

    def __init__(self, **kwargs):
        # self.__dict__ = dict(self.__dict__, **kwargs)
        self.__dict__.update(**kwargs)

RETRY = 0
TIMELIMIT = 0

_continuous = True


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


def store(db, way, update, method):
    def wrap(fun):
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


def handleIndex(workqueue, result, method, args, kwargs, priority, methodId, times):
    index = result.next()
    if index is not None and times == 0:
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
        workqueue.put((priority, methodId, 0, indexargs, indexkwargs))


def handleNextStore(workqueue, retvar, method, hasnext=False, hasstore=False):
    if type(retvar) == dict:
        hasnext and workqueue.put(
            (method.next.priority, id(method.next), 0, (), retvar))
        hasstore and workqueue.put(
            (method.store.priority, id(method.store), 0, (), {'obj': retvar['obj']}))
    elif type(retvar) == tuple:
        hasnext and workqueue.put(
            (method.next.priority, id(method.next), 0, retvar, {}))
        hasstore and workqueue.put(
            (method.store.priority, id(method.store), 0, (retvar[0],), {}))
    else:
        hasstore and workqueue.put(
            (method.store.priority, id(method.store), 0, (retvar,), {}))
        # raise "Incorrect result for next function."


def handleExcept(workqueue, method, args, kwargs, times, methodId, count='fail'):
    if times < method.retry:
        times = times + 1
        workqueue.put((priority, methodId, times, args, kwargs))
    else:
        count = count + 1
        t, v, b = sys.exc_info()
        err_messages = traceback.format_exception(t, v, b)
        _print(method.__name__, ': %s, %s \n' %
               (str(args), str(kwargs)), ','.join(err_messages), '\n')


def geventwork(workqueue):
    while _continuous:
        if workqueue.empty():
            sleep(0.1)
        else:
            timer = Nevertimeout()
            priority, methodId, times, args, kwargs = workqueue.get()
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
                            workqueue, result, method, args, kwargs, priority, methodId, times)
                        for retvar in result:
                            handleNextStore(
                                workqueue, retvar, method, hasattr(method, 'next'), hasattr(method, 'store'))
                        method.succ = method.succ + 1
                    except TimeoutError:
                        handleExcept(
                            workqueue, method, args, kwargs, times, methodId, method.timeout)
                    except:
                        handleExcept(
                            workqueue, method, args, kwargs, times, methodId, method.fail)
                else:
                    handleNextStore(
                        workqueue, result, method, hasattr(method, 'next'), hasattr(method, 'store'))
                    method.succ = method.succ + 1
            except TimeoutError:
                handleExcept(
                    workqueue, method, args, kwargs, times, methodId, method.timeout)
            except:
                handleExcept(
                    workqueue, method, args, kwargs, times, methodId, method.fail)
            finally:
                workqueue.task_done()
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


class Workflows(object):

    """
        任务流
    """

    def __init__(self, worknum, queuetype, worktype):
        if worktype == 'COROUTINE':
            monkey.patch_all(Event=True)
            gid = threading._get_ident()
            threading._active[gid] = threading._active[MTID]
            PriorjoinQueue = GPriorjoinQueue
        else:
            PriorjoinQueue = TPriorjoinQueue
        self.__flowcount = {'inner': set(), 'outer': set()}
        self.__worknum = worknum
        self.__queuetype = queuetype
        self.__worktype = worktype
        if not hasattr(self, 'clsname'):
            self.clsname = str(self.__class__).split(".")[-1].replace("'>", "")
        try:
            if self.__queuetype == 'P':
                self.queue = PriorjoinQueue()
            else:
                self.queue = BeanstalkdQueue(tube=str(id(self)))
        except:
            print 'Wrong type of queue, please choose P or B or start your beanstalkd service.'
        self.workers = []
        self.__flows = {}
        global sleep
        if self.__worktype == 'COROUTINE':
            from gevent import sleep
            for k in range(worknum):
                if self.__queuetype == 'P':
                    worker = functools.partial(geventwork, self.queue)
                else:
                    worker = functools.partial(
                        geventwork, BeanstalkdQueue(tube=str(id(self))))
                self.workers.append(worker)
        else:
            from time import sleep
            for k in range(worknum):
                if self.__queuetype == 'P':
                    worker = Foreverworker(self.queue)
                else:
                    worker = Foreverworker(BeanstalkdQueue(tube=str(id(self))))
                self.workers.append(worker)

    def tinder(self, flow):
        return self.__flows[flow]['tinder']

    def terminator(self, flow):
        return self.__flows[flow]['terminator']

    def addFollow(self, flow, currmethod, nextmethod):
        flag = False
        if self.__flows[flow]['tinder'] is None:
            self.__flows[flow]['tinder'] = currmethod
            self.__flows[flow]['terminator'] = currmethod
        it = self.__flows[flow]['tinder']
        assure(currmethod)
        assure(nextmethod)
        if it == nextmethod:
            flag = True
            if currmethod.priority is None:
                currmethod.priority = nextmethod.priority + 1
            self.__flows[flow]['steps'] = self.__flows[flow]['steps'] + 1
            self.__flows[flow]['hasprior'] = self.__flows[flow][
                'hasprior'] and (currmethod.priority is not None)
        elif it == currmethod:
            flag = True
            self.__flows[flow]['steps'] = 2
            self.__flows[flow]['hasprior'] = (currmethod.priority is not None) and (
                nextmethod.priority is not None)
        else:
            self.__flows[flow]['steps'] = self.__flows[flow]['steps'] + 1
            while hasattr(it, 'next'):
                it = it.next
                if it == currmethod:
                    self.__flows[flow]['hasprior'] = self.__flows[flow][
                        'hasprior'] and (nextmethod.priority is not None)
                    flag = True
        if flag:
            currmethod.next = nextmethod
            if currmethod.next == self.__flows[flow]['tinder']:
                self.__flows[flow]['tinder'] = currmethod
            else:
                self.__flows[flow]['terminator'] == currmethod.next
            self.__flowcount['outer'].add(flow)
            it = self.__flows[flow]['tinder']
            if not self.__flows[flow]['hasprior']:
                num = 0
                it.priority = self.__flows[flow]['steps'] - num
                while hasattr(it, 'next'):
                    it = it.next
                    num = num + 1
                    it.priority = self.__flows[flow]['steps'] - num
                self.__flows[flow]['hasprior'] = True
        else:
            raise "Not find the flow."

    def deleteFollow(self, flow, currmethod):
        flag = False
        it = self.__flows[flow]['tinder']
        if it == currmethod:
            flag = True
        else:
            while hasattr(it, 'next'):
                it = it.next
                if it == currmethod:
                    flag = True
                    break
        if flag and hasattr(currmethod, 'next'):
            if self.__flows[flow]['terminator'] == currmethod.next:
                self.__flows[flow]['terminator'] = currmethod
            if self.__flows[flow]['terminator'] == self.__flows[flow]['tinder']:
                # self.__flows[flow]['tinder'] = self.__flows[flow]['terminator'] = None
                self.__flowcount['outer'].remove(flow)
                del self.__flows[flow]
            delattr(currmethod, 'next')
        else:
            raise "Not fine the flow."

    def extractFlow(self):
        def imitate(p, b):
            if not hasattr(b, '__name__'):
                b.__name__ = str(p).split(' at ')[0].split(' of ')[0].split(
                    '<function ')[-1].split('.')[-1].replace(' ', '').replace('>', '')
            b.succ = 0
            b.fail = 0
            b.timeout = 0
            hasattr(p, 'index') and setattr(b, 'index', p.index)
            hasattr(p, 'store') and setattr(b, 'store', p.store)
            b.retry = (hasattr(p, 'retry') and p.retry) or RETRY
            b.timelimit = (hasattr(p, 'timelimit') and p.timelimit) or TIMELIMIT
            b.priority = (hasattr(p, 'priority') and p.priority) or None
        if self.__flowcount['inner']:
            print "Inner workflow can be set once and has been set."
        else:
            for it in dir(self):
                it = getattr(self, it)
                if hasattr(it, 'label') and hasattr(it, 'next'):
                    self.__flows[it.label] = {'tinder': it, 'terminator': it}
            for flow in self.__flows.values():
                flow['hasprior'] = True
                flow['steps'] = 1
                p = flow['tinder']
                b = functools.partial(p)
                imitate(p, b)
                flow['hasprior'] = flow['hasprior'] and (
                    b.priority is not None)
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
                    flow['terminator'] = b
            for flow in self.__flows.values():
                if not flow['hasprior']:
                    it = flow['tinder']
                    num = 0
                    it.priority = flow['steps'] - num
                    while hasattr(it, 'next'):
                        it = it.next
                        num = num + 1
                        it.priority = flow['steps'] - num
                    flow['hasprior'] = True
            print "Inner workflow is set."

    def fire(self, flow, *args, **kwargs):
        if self.__flows[flow]['tinder'] is not None:
            self.queue.put((self.__flows[flow]['tinder'].priority, id(
                self.__flows[flow]['tinder']), 0, args, kwargs))
            for worker in self.workers:
                if self.__worktype == 'COROUTINE':
                    gevent.spawn(worker)
                else:
                    worker.setDaemon(True)
                    worker.start()
        else:
            raise 'There is no work flow.'

    def exit(self):
        self.queue.task_done(force=True)

    def waitComplete(self):
        self.queue.join()

    def __del__(self):
        del threading._active[MTID]

if __name__ == '__main__':
    pass
