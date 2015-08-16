#!/usr/bin/python
# coding=utf-8
import pickle
import heapq
import beanstalkc
from Queue import PriorityQueue
from gevent.queue import Queue

from character import unicode2utf8

class BQ(object):
    conditions = {}

    def __init__(self, host='localhost', port=11300, tube='default', timeout=30, items=None, unfinished_tasks=None):
        import threading
        self.bc = beanstalkc.Connection(host, port, connect_timeout=timeout)
        self.tube = tube
        self.bc.use(self.tube)
        self.bc.watch(self.tube)
        if self.tube in BQ.conditions:
            pass
        else:
            BQ.conditions[self.tube] = {
                'unfinished_tasks': unfinished_tasks or 0, 'event': threading.Event()}
            self.clear()
            BQ.conditions[self.tube]['event'].set()
        if items:
            for item in items:
                self.put(item)

    def __repr__(self):
        return "<" + str(self.__class__).replace(" ", "").replace("'", "").split('.')[-1]

    def copy(self):
        pass

    def put(self, item):
        index = self.bc.put(pickle.dumps(item), priority=item[0])
        BQ.conditions[self.tube]['unfinished_tasks'] += 1
        BQ.conditions[self.tube]['event'].clear()
        self.setState(item[-1], 'status', 2)
        self.setState(item[-1], 'index', index)

    def get(self):
        item = self.bc.reserve()
        item.delete()
        item = pickle.loads(item.body)
        if self.getState(item[-1], 'status') is None or self.getState(item[-1], 'status') == 2:
            self.setState(item[-1], 'status', 1)
        else:
            item = None
        return item

    def redo(self, taskid, heappush=heapq.heappush):
        item = self.getState(taskid, 'item')
        if item:
            index = self.bc.put(pickle.dumps(item), priority=item[0])
            self.unfinished_tasks += 1
            BQ.conditions[self.tube]['event'].clear()
            self.setState(item[-1], 'status', 2)
            self.setState(item[-1], 'index', index)

    def select(self, taskid, heappop=heapq.heappop):
        index = self.getState(taskid, 'index')
        item = None
        if index:
            item = self.bc.peek(index)
            item.delete()
            item = pickle.loads(item.body)
            if self.getState(item[-1], 'status') == 2:
                self.setState(item[-1], 'status', 1)
        return item

    def empty(self):
        if self.bc.stats_tube(self.tube)['current-jobs-ready'] == 0:
            return True
        else:
            return False

    def clear(self):
        while not self.empty():
            item = self.get()
            del item

    def task_done(self, item=None, force=False):
        if BQ.conditions[self.tube]['unfinished_tasks'] <= 0:
            raise ValueError('task_done() called too many times')
        BQ.conditions[self.tube]['unfinished_tasks'] -= 1
        if item:
            self.recycle(item)
            self.setState(item[-1], 'status', 0)
            self.setState(item[-1], 'index', -1)
            self.setState(item[-1], 'item', item)
        if BQ.conditions[self.tube]['unfinished_tasks'] == 0 or force:
            # if self.empty() or force:
            BQ.conditions[self.tube]['event'].set()

    def join(self):
        BQ.conditions[self.tube]['event'].wait()

    def recycle(self, item):
        pass

    def setState(self, taskid, key, val):
        pass

    def getState(self, taskid, key):
        pass


class GPQ(Queue):

    def __init__(self, maxsize=None, items=None, unfinished_tasks=None):
        from gevent.event import Event
        # Queue.__init__(self, maxsize, items)
        super(GPQ, self).__init__(maxsize, items)
        self.unfinished_tasks = unfinished_tasks or 0
        self._cond = Event()
        self._cond.set()
        self.incount = 0
        self.outcount = 0

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
        self.incount += 1
        self.unfinished_tasks += 1
        self._cond.clear()
        self.setState(item[-1], 'status', 2)
        self.setState(item[-1], 'index', self.incount - 1)

    def _get(self, heappop=heapq.heappop):
        item = heappop(self.queue)
        self.outcount += 1
        if self.getState(item[-1], 'status') is None or self.getState(item[-1], 'status') == 2:
            self.setState(item[-1], 'status', 1)
        else:
            item = None
        return item

    def put(self, item, block=True, timeout=None):
        super(GPQ, self).put(item, block, timeout)
        return self.incount - 1

    def redo(self, taskid, heappush=heapq.heappush):
        item = self.getState(taskid, 'item')
        if item:
            heappush(self.queue, item)
            self.incount += 1
            self.unfinished_tasks += 1
            self._cond.clear()
            self.setState(item[-1], 'status', 2)
            self.setState(item[-1], 'index', self.incount - 1)

    def select(self, taskid, heappop=heapq.heappop):
        index = self.getState(taskid, 'index')
        item = None
        if index:
            item = self.queue[index - self.outcount]
            if self.getState(item[-1], 'status') == 2:
                self.setState(item[-1], 'status', 1)
        return item

    def task_done(self, item=None, force=False):
        if self.unfinished_tasks <= 0:
            raise ValueError('task_done() called too many times')
        self.unfinished_tasks -= 1
        if item:
            self.recycle(item)
            self.setState(item[-1], 'status', 0)
            self.setState(item[-1], 'index', -1)
            self.setState(item[-1], 'item', item)
        if self.unfinished_tasks == 0 or force:
            self._cond.set()

    def join(self):
        self._cond.wait()

    def recycle(self, item):
        pass

    def setState(self, taskid, key, val):
        pass

    def getState(self, taskid, key):
        pass

class TPQ(PriorityQueue):

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
        self.incount = 0
        self.outcount = 0

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
        self.incount += 1
        self.unfinished_tasks += 1
        self._cond.clear()
        self.setState(item[-1], 'status', 2)
        self.setState(item[-1], 'index', self.incount - 1)

    def get(self, heappop=heapq.heappop):
        item = heappop(self.queue)
        self.outcount += 1
        if self.getState(item[-1], 'status') is None or self.getState(item[-1], 'status') == 2:
            self.setState(item[-1], 'status', 1)
        else:
            item = None
        return item

    def redo(self, taskid, heappush=heapq.heappush):
        item = self.getState(taskid, 'item')
        if item:
            heappush(self.queue, item)
            self.incount += 1
            self.unfinished_tasks += 1
            self._cond.clear()
            self.setState(item[-1], 'status', 2)
            self.setState(item[-1], 'index', self.incount - 1)

    def select(self, taskid, heappop=heapq.heappop):
        index = self.getState(taskid, 'index')
        item = None
        if index:
            item = self.queue[index - self.outcount]
            if self.getState(item[-1], 'status') == 2:
                self.setState(item[-1], 'status', 1)
        return item

    def task_done(self, item=None, force=False):
        if self.unfinished_tasks <= 0:
            raise ValueError('task_done() called too many times')
        self.unfinished_tasks -= 1
        if item:
            self.recycle(item)
            self.setState(item[-1], 'status', 0)
            self.setState(item[-1], 'index', -1)
            self.setState(item[-1], 'item', item)
        if self.unfinished_tasks == 0 or force:
            self._cond.set()

    def join(self):
        self._cond.wait()

    def recycle(self, item):
        pass

    def setState(self, taskid, key, val):
        pass

    def getState(self, taskid, key):
        pass

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
