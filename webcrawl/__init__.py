#!/usr/bin/env python
# coding=utf8

__import__('pkg_resources').declare_namespace(__name__)
__version__ = '0.0.2'
__author__ = 'hk'

DEFAULT = [
'__call__',
'__class__',
'__delattr__',
'__dict__',
'__doc__',
'__format__',
'__getattribute__',
'__hash__',
'__init__',
'__new__',
'__reduce__',
'__reduce_ex__',
'__repr__',
'__setattr__',
'__setstate__',
'__sizeof__',
'__str__',
'__subclasshook__'
]

SPACE = 100
RETRY = 0
TIMELIMIT = 0
CONTINUOUS = True

class Enum(object):

    def __init__(self, **kwargs):
        self.__dict__.update(**kwargs)

    def __getattribute__(self, key):
        return object.__getattribute__(self, key)

    def __setattr__(self, key, val):
        raise Exception("AttributeError: '%s' object has no attribute '%s'" % (self.__class__.__name__, key))

class MyLocal(object):

    def __init__(self, **kwargs):
        # self.__dict__ = dict(self.__dict__, **kwargs)
        self.__dict__.update(**kwargs)

    def update(self, **kwargs):
        self.__dict__.update(**kwargs)


class Logger(object):

    @classmethod
    def _print(self, **kwargs):
        for key in kwargs:
            print key, ': ', kwargs[key]
