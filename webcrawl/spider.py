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


class SpiderOrigin(Workflows):
    __lasttime = datetime.datetime.now()
    __lock = threading.Lock()

    def __init__(self, worknum=WORKNUM, queuetype=QUEUETYPE, timeout=-1, tid=0, settings={}):
        super(SpiderOrigin, self).__init__(worknum=worknum, queuetype=queuetype, tid=tid, settings=settings)
        self.timeout = timeout
        self.extract()

    def fetch(self, flow, step, version, *args, **kwargs):
        start = time.time()
        self.fire(flow, step, version, *args, **kwargs)
        if self.timeout > -1:
            def check(self, timeout):
                time.sleep(timeout)
                self.exit()
                print 'Time out of %s. ' % str(self.timeout)
            watcher = Thread(
                target=check, args=(self, self.timeout - (time.time() - start)))
            watcher.setDaemon(True)
            watcher.start()
        self.wait()
        it = self.select(flow)
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
        end = time.time()
        self.totaltime = end - start

    def __del__(self):
        pass

if __name__ == '__main__':
    pass
