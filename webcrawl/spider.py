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

from task import Workflows, CFG

WORKNUM = 30


class SpiderOrigin(Workflows):

    def __init__(self, worknum=WORKNUM, queuetype=CFG.L, timeout=-1, tid=0, settings={}):
        super(SpiderOrigin, self).__init__(worknum=worknum, queuetype=queuetype, tid=tid, settings=settings)
        self.timeout = timeout

    def format_section(self, section):
        if section is None:
            return section
        return '%s.%s' % (str(self), section)

    def fetch(self, flow, section, version, *args, **kwargs):
        start = time.time()
        step = self.select(flow, self.format_section(section))
        self.fire(flow, step, version, *args, **kwargs)
        if self.timeout > -1:
            def check(self, timeout):
                time.sleep(timeout)
                self.exit()
                print 'Time out of %s. ' % str(self.timeout)
            watcher = Thread(target=check, args=(self, self.timeout - (time.time() - start)))
            watcher.setDaemon(True)
            watcher.start()
        self.wait()
        for fun in self.traversal(step):
            if hasattr(fun, 'store'):
                try:
                    fun.store(None, maxsize=0)
                except:
                    t, v, b = sys.exc_info()
                    err_messages = traceback.format_exception(t, v, b)
                    print(': %s, %s \n' % (str(args), str(kwargs)),
                          ','.join(err_messages), '\n')
        end = time.time()
        self.totaltime = end - start

    def __del__(self):
        pass

if __name__ == '__main__':
    pass
