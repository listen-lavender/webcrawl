#!/usr/bin/env python
# coding=utf-8
import weakref
import time
import datetime
import sys
import traceback
import functools
import threading
from threading import Thread


from task import Workflows

WORKNUM = 30
QUEUETYPE = 'P'
WORKTYPE = 'COROUTINE'


class SpiderOrigin(Workflows):
    __lasttime = datetime.datetime.now()
    __lock = threading.Lock()

    def __init__(self, worknum=WORKNUM, queuetype=QUEUETYPE, worktype=WORKTYPE, timeout=-1, tid=0):
        super(SpiderOrigin, self).__init__(worknum=worknum, queuetype=queuetype, worktype=worktype, tid=tid)
        # Workflows.__init__(self, worknum=worknum, queuetype=queuetype, worktype=worktype)
        # Keeper.__init__(self)
        self.timeout = timeout
        self.dones = set()
        self.extractFlow()

    def fetchDatas(self, flow, step=0, *args, **kwargs):
        try:
            start = time.time()
            self.fire(flow, step, *args, **kwargs)
            if self.timeout > -1:
                def check(self, timeout):
                    time.sleep(timeout)
                    self.exit()
                    print 'Time out of %s. ' % str(self.timeout)
                watcher = Thread(
                    target=check, args=(self, self.timeout - (time.time() - start)))
                watcher.setDaemon(True)
                watcher.start()
            self.waitComplete()
            it = self.tinder(flow)
            while True:
                if hasattr(it, 'store'):
                    try:
                        it.store(None, maxsize=0)
                    except:
                        t, v, b = sys.exc_info()
                        err_messages = traceback.format_exception(t, v, b)
                        print(': %s, %s \n' % (str(args), str(kwargs)),
                              ','.join(err_messages), '\n')
                if hasattr(it, 'next'):
                    it = it.next
                else:
                    break
            self.dones.add(flow)
            end = time.time()
            self.totaltime = end - start
            return True
        except:
            return False

    def clearDataOne(self, one):
        pass

    def implementDataone(self, *args, **kwargs):
        pass

    @classmethod
    def uniquetime(cls, timespan=1, lasttime=None):
        if lasttime is None:
            with cls.__lock:
                cls.__lasttime = cls.__lasttime + \
                    datetime.timedelta(seconds=timespan)
                return cls.__lasttime
        else:
            cls.__lasttime = max(cls.__lasttime, lasttime)

    def statistic(self):
        for flow in self.dones:
            it = self.tinder(flow)
            self.stat = {'total': {'succ': 0, 'fail': 0, 'timeout': 0}}
            self.stat[it.__name__] = {}
            self.stat[it.__name__]['succ'] = it.succ
            self.stat[it.__name__]['fail'] = it.fail
            self.stat[it.__name__]['timeout'] = it.timeout
            self.stat['total']['succ'] += it.succ
            self.stat['total']['fail'] += it.fail
            self.stat['total']['timeout'] += it.timeout
            if hasattr(it, 'store'):
                self.stat['total']['succ'] += it.store.succ
                self.stat['total']['fail'] += it.store.fail
                self.stat['total']['timeout'] += it.store.timeout
            #     print it.store.__name__, 'succ: ', it.store.succ
            #     print it.store.__name__, 'fail: ', it.store.fail
            #     print it.store.__name__, 'timeout: ', it.store.timeout
            while hasattr(it, 'next'):
                self.stat[it.next.__name__] = {}
                self.stat[it.next.__name__]['succ'] = it.next.succ
                self.stat[it.next.__name__]['fail'] = it.next.fail
                self.stat[it.next.__name__]['timeout'] = it.next.timeout
                self.stat['total']['succ'] += it.next.succ
                self.stat['total']['fail'] += it.next.fail
                self.stat['total']['timeout'] += it.next.timeout
                if hasattr(it.next, 'store'):
                    self.stat['total']['succ'] += it.next.store.succ
                    self.stat['total']['fail'] += it.next.store.fail
                    self.stat['total']['timeout'] += it.next.store.timeout
                #     print it.next.store.__name__, 'succ: ', it.next.store.succ
                #     print it.next.store.__name__, 'fail: ', it.next.store.fail
                #     print it.next.store.__name__, 'timeout: ', it.next.store.timeout
                it = it.next

    def now():
        return datetime.datetime.now()

    def __del__(self):
        pass

if __name__ == '__main__':
    from threading import Thread, currentThread

    class AB(SpiderOrigin):

        def __init__(self, worknum=WORKNUM, queuetype=QUEUETYPE, worktype=WORKTYPE, timeout=-1):
            super(AB, self).__init__(
                worknum=worknum, queuetype=queuetype, worktype=worktype)

    class CD(object):

        def __init__(self):
            pass

        def run(self, name, nums, times):
            for k in range(nums):
                time.sleep(times)
                print name, AB.uniquetime()

    cd = CD()
    cdts = []
    for k in range(10):
        cdt = Thread(
            target=cd.run, args=('thread%d' % k, k + 1, (10 - k) * 0.1))
        cdts.append(cdt)
        cdt.start()
    for cdt in cdts:
        cdt.join()
