#!/usr/bin/python
# coding=utf-8

class OriginError(Exception):
    def __init__(self):
        pass

    def __del__(self):
        pass

class URLFailureException(OriginError):
    def __init__(self, url, respcode):
        self.url = url
        self.respcode = respcode

    def log(self):
        print('%s: %s' % (self.url, self.respcode))

class TimeoutError(OriginError):
    def __init__(self):
        pass

    def __del__(self):
        pass
