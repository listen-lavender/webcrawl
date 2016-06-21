#!/usr/bin/env python
# coding=utf-8

"""
   tools of wrapped requests or related things
"""
import json
import requests
import urllib2
import StringIO
import functools
import threading
import time

from queue.lib.queue import Queue
from lxml import etree as ET
from lxml import html as HT
from character import unicode2utf8
from . import MyLocal
from exception import URLFailureException, MarktypeError, FormatError, ArgumentError

try:
    import Image
except:
    try:
        from PIL import Image
    except:
        print "You need install PIL library, from http://www.pythonware.com/products/pil/."


REQU = MyLocal(timeout=30)

PROXY = MyLocal(url='', queue=Queue(), choose=lambda :[], log=lambda pid, elapse:None, use=False, worker=None)

FILE = MyLocal(make=True, dir='')

class Proxyworker(threading.Thread):

    def run(self):
        while True:
            if PROXY.queue.empty():
                time.sleep(0.5)
            else:
                pid, elapse = PROXY.queue.get()
                PROXY.log(pid, elapse)

class Fakeresponse(object):
    def __init__(self, url, status_code, content, text, headers, cookies, js_result=None):
        self.url = url
        self.status_code = status_code
        self.content = content
        self.text = text
        self.headers = headers
        self.cookies = cookies
        self.js_result = js_result

class Fakerequest(object):
    def __init__(self, url, headers=None, cookies=None, javascript={'start':None, 'end':None, 'request':None, 'receive':None}, load_images=False, timeout=None, width=1024, height=768, wait=1):
        self.url = url
        self.headers = headers
        self.cookies = cookies
        self.javascript = javascript
        self.load_images = load_images
        self.timeout = timeout
        self.width = width
        self.height = height
        self.wait = wait

PROXY.worker = Proxyworker()

def contentFilter(content):
    return content

def byProxy(fun):
    @functools.wraps(fun)
    def wrapper(*args, **kwargs):
        proxy = None
        if PROXY.use:
            start = time.time()
            proxy = PROXY.choose()
            kwargs['proxies'] = {
                "http": kwargs['proxies'] if 'proxies' in kwargs else "http://%s:%s" % (proxy['ip'], proxy['port'])}
            kwargs['timeout'] = REQU.timeout if kwargs.get(
                'timeout') is None else max(kwargs['timeout'], REQU.timeout)
        result = fun(*args, **kwargs)
        if proxy:
            end = time.time()
            PROXY.queue.put(proxy['id'], round(end-start, 6))
        return result
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

def getJsonNodeContent(node, consrc):
    """
    """
    return ''

def requformat(r, coding, dirtys, myfilter, format, filepath):
    code = r.status_code
    dlf = r.content
    content = r.content
    content = content.decode(coding, 'ignore').encode('utf-8')
    if not code in [200, 301, 302]:
        raise URLFailureException(r.url, code)
    for one in dirtys:
        content = content.replace(one[0], one[1])
    content = myfilter(content)
    if format == 'HTML':
        content = HT.fromstring(content.decode('utf-8'))
    elif format == 'JSON':
        content = unicode2utf8(json.loads(content.decode('utf-8')))
    elif format == 'XML':
        content = ET.fromstring(content)
    elif format == 'TEXT':
        content = content
    elif format == 'ORIGIN':
        content = r
    else:
        raise FormatError(format)
    if FILE.make and filepath is not None:
        fi = open(FILE.dir + filepath, 'w')
        fi.write(dlf)
        fi.close()
    return content


@byProxy
def get(url, headers=None, cookies=None, proxies=None, timeout=10, allow_redirects=True, coding='utf-8', dirtys=[], myfilter=contentFilter, format='ORIGIN', filepath=None, s=None, browse=None):
    """
    """
    if browse is not None:
        package = {'load_images': browse.load_images, 'method': 'GET'}
        package['url'] = url
        package['urlhash'] = hash(url)
        package['allow_redirects'] = allow_redirects
        package['headers'] = headers or browse.headers
        package['cookies'] = cookies or browse.cookies
        package['javascript'] = browse.javascript
        package['width'] = browse.width
        package['height'] = browse.height
        package['timeout'] = browse.timeout or int(timeout * 0.95)
        package['wait'] = browse.wait
        if cookies:
            package['headers']['Cookie'] = '; '.join(['%s=%s' % (key, val) for key, val in cookies.items()])
        r = requests.post(browse.url, json.dumps(package), timeout=timeout)
        content = r.content
        for one in dirtys:
            content = content.replace(one[0], one[1])
        dirtys = []
        content = unicode2utf8(json.loads(content.decode('utf-8')))
        r = Fakeresponse(content['url'], content['status_code'], content['content'] or content['error'], r.content.decode('utf-8'), content['headers'], content['cookies'], js_result=content['js_result'])
    elif s is None:
        r = requests.get(url, headers=headers, cookies=cookies,
                         proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    else:
        r = s.get(url, headers=headers, cookies=cookies, proxies=proxies,
                  timeout=timeout, allow_redirects=allow_redirects)
    return requformat(r, coding, dirtys, myfilter, format, filepath)


@byProxy
def post(url, data=None, files=None, headers=None, cookies=None, proxies=None, timeout=10, allow_redirects=True, coding='utf-8', dirtys=[], myfilter=contentFilter, format='ORIGIN', filepath=None, s=None, browse=None):
    """
    """
    if data is None and files is None:
        raise ArgumentError()
    if browse is not None:
        package = {'load_images': browse.load_images, 'method': 'POST'}
        package['url'] = url
        package['urlhash'] = hash(url)
        package['data'] = data
        package['allow_redirects'] = allow_redirects
        package['headers'] = headers or browse.headers
        package['cookies'] = cookies or browse.cookies
        package['javascript'] = browse.javascript
        package['width'] = browse.width
        package['height'] = browse.height
        package['timeout'] = browse.timeout or int(timeout * 0.95)
        package['wait'] = browse.wait
        if cookies:
            package['headers']['Cookie'] = '; '.join(['%s=%s' % (key, val) for key, val in cookies.items()])
        r = requests.post(browse.url, json.dumps(package), timeout=timeout)
        content = r.content
        for one in dirtys:
            content = content.replace(one[0], one[1])
        dirtys = []
        content = unicode2utf8(json.loads(content.decode('utf-8')))
        r = Fakeresponse(content['url'], content['status_code'], content['content'] or content['error'], r.content.decode('utf-8'), content['headers'], content['cookies'], js_result=content['js_result'])
    elif s is None:
        r = requests.post(url, data=data, files=files, headers=headers, cookies=cookies,
                          proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    else:
        r = s.post(url, data=data, files=files, headers=headers, cookies=cookies,
                   proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    return requformat(r, coding, dirtys, myfilter, format, filepath)


@byProxy
def head(url, headers=None, cookies=None, proxies=None, timeout=10, allow_redirects=True, coding='utf-8', dirtys=[], myfilter=contentFilter, format='ORIGIN', filepath=None, s=None):
    """
    """
    if s is None:
        r = requests.head(url, headers=headers, cookies=cookies,
                          proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    else:
        r = s.head(url, headers=headers, cookies=cookies,
                   proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    return requformat(r, coding, dirtys, myfilter, format, filepath)


def downloadImg(url, tofile=None):
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
    print 'end...'
