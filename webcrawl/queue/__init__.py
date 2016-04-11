#!/usr/bin/python
# coding=utf8

__import__('pkg_resources').declare_namespace(__name__)
__version__ = '1.0.3'
__author__ = 'hk'

class MyLocal(object):

    def __init__(self, **kwargs):
        # self.__dict__ = dict(self.__dict__, **kwargs)
        self.__dict__.update(**kwargs)

    def update(self, **kwargs):
        self.__dict__.update(**kwargs)
