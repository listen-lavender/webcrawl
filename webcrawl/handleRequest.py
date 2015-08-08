#!/usr/bin/python
# coding=utf-8

"""
   tools of wrapped requests or related things
"""
import json
import requests
import random
import urllib2
import Image
import StringIO
import functools
import threading

from lxml import etree as ET
from lxml import html as HT
from character import unicode2utf8
from work import MyLocal
from exception import URLFailureException, MarktypeError, FormatError

proxies = []

REQU = MyLocal(timeout=30)

PROXY = MyLocal(url='', fun=lambda :[], use=False)

FILE = MyLocal(make=True, dir='')


def contentFilter(contents):
    return contents


def chooseProxy():
    global proxies
    if not proxies:
        proxies = PROXY.fun()
    return proxies


def byProxy(fun):
    @functools.wraps(fun)
    def wrapper(*args, **kwargs):
        if PROXY.use:
            proxies = chooseProxy()
            proxy = random.choice(proxies)
            kwargs['proxies'] = {
                "http": "http://%s:%s" % (proxy['ip'], proxy['port'])} if not 'proxies' in kwargs else kwargs['proxies']
            kwargs['timeout'] = REQU.timeout if kwargs.get(
                'timeout') is None else max(kwargs['timeout'], REQU.timeout)
        return fun(*args, **kwargs)
    return wrapper


def getNodeContent(node, consrc, marktype='HTML'):
    """
    """
    if node is not None:
        if consrc == 'TEXT':
            if marktype == 'HTML':
                retvar = node.text_content() or ''
            elif marktype == 'XML':
                retvar = node.text or ''
            else:
                raise MarktypeError(marktype)
        else:
            retvar = node.get(consrc['ATTR']) or ''
        retvar = retvar.encode('utf-8')
    else:
        retvar = ''
    return retvar.strip()


def getHtmlNodeContent(node, consrc):
    """
    """
    return getNodeContent(node, consrc, 'HTML')


def getXmlNodeContent(node, consrc):
    """
    """
    return getNodeContent(node, consrc, 'XML')


def requformat(r, coding, dirtys, myfilter, format, filepath):
    code = r.status_code
    contents = r.content
    contents = contents.decode(coding, 'ignore').encode('utf-8')
    if not code in [200, 301, 302]:
        raise URLFailureException(r.url, code)
    for one in dirtys:
        contents = contents.replace(one[0], one[1])
    contents = myfilter(contents)
    if format == 'HTML':
        content = HT.fromstring(contents.decode('utf-8'))
    elif format == 'JSON':
        content = unicode2utf8(json.loads(contents.decode('utf-8')))
    elif format == 'XML':
        content = ET.fromstring(contents)
    elif format == 'TEXT':
        content = contents
    elif format == 'ORIGIN':
        content = r
    else:
        raise FormatError(format)
    if FILE.make and filepath is not None:
        fi = open(FILE.dir + filepath, 'w')
        fi.write(contents)
        fi.close()
    return content


@byProxy
def requGet(url, headers=None, cookies=None, proxies=None, timeout=10, allow_redirects=True, coding='utf-8', dirtys=[], myfilter=contentFilter, format='ORIGIN', filepath=None, s=None):
    """
    """
    if s is None:
        r = requests.get(url, headers=headers, cookies=cookies,
                         proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    else:
        r = s.get(url, headers=headers, cookies=cookies, proxies=proxies,
                  timeout=timeout, allow_redirects=allow_redirects)
    return requformat(r, coding, dirtys, myfilter, format, filepath)


@byProxy
def requPost(url, data, headers=None, cookies=None, proxies=None, timeout=10, allow_redirects=True, coding='utf-8', dirtys=[], myfilter=contentFilter, format='ORIGIN', filepath=None, s=None):
    """
    """
    if s is None:
        r = requests.post(url, data=data, headers=headers, cookies=cookies,
                          proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    else:
        r = s.post(url, data=data, headers=headers, cookies=cookies,
                   proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    return requformat(r, coding, dirtys, myfilter, format, filepath)


@byProxy
def requHead(url, headers=None, cookies=None, proxies=None, timeout=10, allow_redirects=True, coding='utf-8', dirtys=[], myfilter=contentFilter, format='ORIGIN', filepath=None, s=None):
    """
    """
    if s is None:
        r = requests.head(url, headers=headers, cookies=cookies,
                          proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    else:
        r = s.head(url, headers=headers, cookies=cookies,
                   proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    return requformat(r, coding, dirtys, myfilter, format, filepath)


def requImg(url, tofile=None):
    """
    """
    r = urllib2.Request(url)
    img_data = urllib2.urlopen(r).read()
    img_buffer = StringIO.StringIO(img_data)
    img = Image.open(img_buffer)
    if FILE.make and tofile is not None:
        img.save(FILE.dir + tofile)
    return img


def tree(content, coding='unicode', marktype='HTML'):
    """
    """
    treefuns = {'HTML': HT.fromstring, 'XML': ET.fromstring}
    if coding is None or coding == 'unicode':
        pass
    else:
        content = content.decode(coding, 'ignore')
    try:
        return treefuns[marktype](content)
    except:
        raise MarktypeError(marktype)


def treeHtml(content, coding='unicode'):
    """
    """
    return tree(content, coding, 'HTML')


def treeXml(content, coding='unicode'):
    """
    """
    return tree(content, coding, 'XML')

if __name__ == '__main__':
    print 'start...'
    print 'hkhkh', requHead('http://www.homeinns.com/hotel/027060')
    print 'end...'
