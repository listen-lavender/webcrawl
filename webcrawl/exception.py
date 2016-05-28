#!/usr/bin/env python
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
        print('%s: %s' % (self.url, self.respcode))

    def __del__(self):
        pass


class ArgumentError(OriginError):

    def __init__(self):
        print('Need data or file for post.')

    def __del__(self):
        pass


class TimeoutError(OriginError):

    def __init__(self):
        print('Timeout error.')

    def __del__(self):
        pass


class MarktypeError(OriginError):

    def __init__(self, marktype):
        self.marktype = marktype
        print('Unknow marktype: %s.' % self.marktype)

    def __del__(self):
        pass


class FormatError(OriginError):

    def __init__(self, format):
        self.format = format
        print('Unknow response type: %s.' % self.format)

    def __del__(self):
        pass

