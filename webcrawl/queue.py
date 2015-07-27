#!/usr/bin/python
# coding=utf-8
import json
import heapq
import beanstalkc
from Queue import PriorityQueue
from gevent.queue import Queue

from character import unicode2utf8


class BeanstalkdQueue(object):
    conditions = {}

    def __init__(self, host='localhost', port=11300, tube='default', timeout=30, items=None, unfinished_tasks=None):
        import threading
        self.bc = beanstalkc.Connection(host, port, connect_timeout=timeout)
        self.tube = tube
        self.bc.use(self.tube)
        self.bc.watch(self.tube)
        if self.tube in BeanstalkdQueue.conditions:
            pass
        else:
            BeanstalkdQueue.conditions[self.tube] = {
                'unfinished_tasks': unfinished_tasks or 0, 'event': threading.Event()}
            self.clear()
            BeanstalkdQueue.conditions[self.tube]['event'].set()
        if items:
            for item in items:
                self.put(item)

    def put(self, item):
        priority, methodId, times, args, kwargs = item
        self.bc.put(json.dumps({'priority': priority, 'methodId': methodId,
                                'times': times, 'args': args, 'kwargs': kwargs}), priority=priority)
        BeanstalkdQueue.conditions[self.tube]['unfinished_tasks'] += 1
        BeanstalkdQueue.conditions[self.tube]['event'].clear()

    def get(self):
        item = self.bc.reserve()
        item.delete()
        item = unicode2utf8(json.loads(item.body))
        return (item['priority'], item['methodId'], item['times'], tuple(item['args']), item['kwargs'])

    def empty(self):
        if self.bc.stats_tube(self.tube)['current-jobs-ready'] == 0:
            return True
        else:
            return False

    def copy(self):
        pass

    def task_done(self, force=False):
        if BeanstalkdQueue.conditions[self.tube]['unfinished_tasks'] <= 0:
            raise ValueError('task_done() called too many times')
        BeanstalkdQueue.conditions[self.tube]['unfinished_tasks'] -= 1
        if BeanstalkdQueue.conditions[self.tube]['unfinished_tasks'] == 0 or force:
            # if self.empty() or force:
            BeanstalkdQueue.conditions[self.tube]['event'].set()

    def join(self):
        BeanstalkdQueue.conditions[self.tube]['event'].wait()

    def clear(self):
        while not self.empty():
            item = self.get()
            del item

    def __repr__(self):
        return "<" + str(self.__class__).replace(" ", "").replace("'", "").split('.')[-1]


class GPriorjoinQueue(Queue):

    def __init__(self, maxsize=None, items=None, unfinished_tasks=None):
        from gevent.event import Event
        Queue.__init__(self, maxsize, items)
        self.unfinished_tasks = unfinished_tasks or 0
        self._cond = Event()
        self._cond.set()

    def _init(self, maxsize, items=None):
        if items:
            self.queue = list(items)
        else:
            self.queue = []

    def copy(self):
        return type(self)(self.maxsize, self.queue, self.unfinished_tasks)

    def _format(self):
        result = Queue._format(self)
        if self.unfinished_tasks:
            result += ' tasks=%s _cond=%s' % (
                self.unfinished_tasks, self._cond)
        return result

    def _put(self, item, heappush=heapq.heappush):
        heappush(self.queue, item)
        # Queue._put(self, item)
        self.unfinished_tasks += 1
        self._cond.clear()

    def _get(self, heappop=heapq.heappop):
        return heappop(self.queue)

    def task_done(self, force=False):
        if self.unfinished_tasks <= 0:
            raise ValueError('task_done() called too many times')
        self.unfinished_tasks -= 1
        if self.unfinished_tasks == 0 or force:
            self._cond.set()

    def join(self):
        self._cond.wait()


class TPriorjoinQueue(PriorityQueue):

    def _init(self, maxsize=None, items=None, unfinished_tasks=None):
        self.maxsize = maxsize or 0
        if items:
            self.queue = list(items)
        else:
            self.queue = []
        from threading import Event
        self.unfinished_tasks = unfinished_tasks or 0
        self._cond = Event()
        self._cond.set()

    def copy(self):
        return type(self)(self.maxsize, self.queue, self.unfinished_tasks)

    def _format(self):
        result = Queue._format(self)
        if self.unfinished_tasks:
            result += ' tasks=%s _cond=%s' % (
                self.unfinished_tasks, self.all_tasks_done)
        return result

    def put(self, item, heappush=heapq.heappush):
        heappush(self.queue, item)
        # Queue._put(self, item)
        self.unfinished_tasks += 1
        self._cond.clear()

    def get(self, heappop=heapq.heappop):
        return heappop(self.queue)

    def task_done(self, force=False):
        if self.unfinished_tasks <= 0:
            raise ValueError('task_done() called too many times')
        self.unfinished_tasks -= 1
        if self.unfinished_tasks == 0 or force:
            self._cond.set()

    def join(self):
        self._cond.wait()

if __name__ == '__main__':
    from gevent.queue import JoinableQueue
    import gevent
    q = PriorjoinQueue()

    def worker():
        while True:
            item = q.get()
            try:
                gevent.sleep(5)
            finally:
                q.task_done()

    def consume():
        for i in range(10):
            gevent.spawn(worker)

    def produce():
        for item in range(20):
            # print 'kkkkkk', 20 - item, item
            q.put((20 - item, item))
            gevent.sleep(0.1)

    import threading
    consume()
    produce()
    # a = threading.Thread(target=consume)
    # b = threading.Thread(target=produce)
    # a.start()
    # b.start()

    q.join()
    print 'kkkkkk'
