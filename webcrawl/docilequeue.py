#!/usr/bin/python
# coding=utf-8
from wildqueue import BQ as WBQ
from wildqueue import GPQ as WGPQ
from wildqueue import TPQ as WTPQ
from trainer import Record

class BQ(WBQ, Record):
    pass

class GPQ(WGPQ, Record):
    pass

class TPQ(WTPQ, Record):
    pass

if __name__ == '__main__':
    print 'kkkkkk'
