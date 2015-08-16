#!/usr/bin/python
# coding=utf-8
import redis

class Record(object):
    conditions = {}

    def __init__(self, host='localhost', port=6379, tube='default', timeout=30, items=None, unfinished_tasks=None):
        pass

    def __repr__(self):
        return "<" + str(self.__class__).replace(" ", "").replace("'", "").split('.')[-1]

    def copy(self):
        pass

    def recycle(self, item):
        pass

    def setState(self, taskid, key, val):
        pass

    def getState(self, taskid, key):
        pass

if __name__ == '__main__':
    print 'kkkkkk'
