#!/usr/bin/python
# coding=utf-8
from wildqueue import BQ as WBQ
from wildqueue import GPQ as WGPQ
from wildqueue import TPQ as WTPQ
from trainer import Record

class BQ(Record, WBQ):
    def __init__(self, host='localhost', port=11300, tube='default', timeout=30, items=None, unfinished_tasks=None):
        WBQ.__init__(self, host, port, tube, timeout, items, unfinished_tasks)
        Record.__init__(self)

class GPQ(Record, WGPQ):
    def __init__(self, maxsize=None, items=None, unfinished_tasks=None):
        WGPQ.__init__(self, maxsize, items, unfinished_tasks)
        Record.__init__(self)

class TPQ(Record, WTPQ):
    def __init__(self, maxsize=None, items=None, unfinished_tasks=None):
        WTPQ.__init__(self, maxsize, items, unfinished_tasks)
        Record.__init__(self)

if __name__ == '__main__':
    print 'kkkkkk'
