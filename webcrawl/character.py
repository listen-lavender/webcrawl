#!/usr/bin/env python
# coding=utf-8

"""
    字符工具
"""

import time
import json
import collections
import functools
from datetime import datetime, date

class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        else:
            return json.JSONEncoder.default(self, obj)

json.dumps = functools.partial(json.dumps, cls=DatetimeEncoder)


def Enum(**enums):
    return type('Enum', (), enums)


def unicode2utf8(obj):
    if isinstance(obj, unicode):
        return obj.encode('utf8')
    if isinstance(obj, str):
        return obj
    elif isinstance(obj, collections.Mapping):
        return dict(map(unicode2utf8, obj.iteritems()))
    elif isinstance(obj, collections.Iterable):
        return type(obj)(map(unicode2utf8, obj))
    else:
        return obj


def _cs(obj, encoding='utf8'):
    if isinstance(obj, unicode):
        return obj.encode(encoding)
    elif isinstance(obj, str):
        return obj
    else:
        return str(obj)


def _cu(string, encoding='utf8'):
    if isinstance(string, unicode):
        return string
    elif isinstance(string, str):
        try:
            return string.decode(encoding)
        except:
            import chardet
            det = chardet.detect(string)
            if det['encoding']:
                return string.decode(det['encoding'], 'ignore')
            else:
                return string.decode('gbk', 'ignore')
    else:
        return unicode(string)


if __name__ == '__main__':
    pass
