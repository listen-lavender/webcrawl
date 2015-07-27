#!/usr/bin/python
# coding=utf-8

"""
    字符工具
"""

import time
import collections


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


def strQ2B(ustring):
    rstring = ""
    if isinstance(ustring, str):
        ustring = _cu(ustring)
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 0x3000:
            inside_code = 0x0020
        else:
            inside_code -= 0xfee0
        if inside_code < 0x0020 or inside_code > 0x7e:
            rstring += uchar
        else:
            rstring += unichr(inside_code)
    return rstring


def isdtiformat(dtistr, dtifmt):
    dtistr = dtistr or ''
    dtifmt = dtifmt or ''
    try:
        time.strptime(dtistr, dtifmt)
        return True
    except:
        return False

if __name__ == '__main__':
    pass
