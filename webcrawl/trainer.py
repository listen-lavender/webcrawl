#!/usr/bin/python
# coding=utf-8
import redis
import pickle

class Record(object):
    conditions = {}

    def __init__(self, host='localhost', port=6379, db=0):
        self.connect = redis.Redis(host, port, db)

    def __repr__(self):
        pass

    def copy(self):
        pass

    def setState(self, taskid, key, val):
        self.connect.hset(taskid, key, pickle.dumps(val))

    def getState(self, taskid, key, convert=True):
        result = self.connect.hget(taskid, key)
        if convert:
            result = pickle.loads(result)
        return result

if __name__ == '__main__':
    print 'kkkkkk'
