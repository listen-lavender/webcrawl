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

myproxys = []

CONS = MyLocal(
    PROXYURL='', PROXYTIMEOUT=30, USEPROXYS=False, FILEMAKE=True, FILEDIR='')


def contentFilter(contents):
    return contents


def chooseProxy():
    global myproxys
    if myproxys:
        return myproxys
    else:
        r = requests.get(CONS.PROXYURL)
        proxys = json.loads(r.content)
        proxys = unicode2utf8(proxys)
        proxys.sort(cmp=lambda x, y: cmp(x[2], y[2]))
        for proxy in proxys:
            #["50.70.48.217", "8080", 12.131555, "00", "11110"]
            proxyip, proxyport, speed, area, cls = proxy
            if float(speed) < 10 and cls.startswith('0'):
                myproxys.append(proxy)
            if float(speed) >= 10:
                break
        return myproxys


def byProxys(fun):
    @functools.wraps(fun)
    def wrapper(*args, **kwargs):
        if CONS.USEPROXYS:
            myproxys = chooseProxy()
            proxy = random.choice(myproxys)
            kwargs['proxies'] = {
                "http": "http://%s:%s" % (proxy[0], proxy[1])} if not 'proxies' in kwargs else kwargs['proxies']
            kwargs['timeout'] = CONS.PROXYTIMEOUT if kwargs.get(
                'timeout') is None else max(kwargs['timeout'], CONS.PROXYTIMEOUT)
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
        raise URLFailureException(url, code)
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
    if CONS.FILEMAKE and filepath is not None:
        fi = open(CONS.FILEDIR + filepath, 'w')
        fi.write(contents)
        fi.close()
    return content


@byProxys
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


@byProxys
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


@byProxys
def requHead(url, headers=None, cookies=None, proxies=None, timeout=10, allow_redirects=True, coding='utf-8', dirtys=[], myfilter=contentFilter, format='ORIGIN', filepath=None, s=None):
    """
    """
    if s is None:
        r = requests.head(url, data=data, headers=headers, cookies=cookies,
                          proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    else:
        r = s.head(url, data=data, headers=headers, cookies=cookies,
                   proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    return requformat(r, coding, dirtys, myfilter, format, filepath)


def requImg(url, tofile=None):
    """
    """
    r = urllib2.Request(url)
    img_data = urllib2.urlopen(r).read()
    img_buffer = StringIO.StringIO(img_data)
    img = Image.open(img_buffer)
    if CONS.FILEMAKE and tofile is not None:
        img.save(CONS.FILEDIR + tofile)
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
    tree(content, coding, 'HTML')


def treeXml(content, coding='unicode'):
    """
    """
    tree(content, coding, 'XML')

if __name__ == '__main__':
    print 'start...'
    print 'hkhkh', requHead('http://www.homeinns.com/hotel/027060')
    print 'end...'
