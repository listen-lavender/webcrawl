#!/usr/bin/env python
# coding=utf8

__import__('pkg_resources').declare_namespace(__name__)
__version__ = '0.0.1'
__author__ = 'hk'


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