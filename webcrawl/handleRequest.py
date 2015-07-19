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
from exception import URLFailureException

myproxys = []

CONS = MyLocal(PROXYURL='http://app.maopao.com:7777/listallproxy', PROXYTIMEOUT=30, USEPROXYS=False, FILEMAKE=True, FILEDIR='/home/hotelinfo/file/')

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
        proxys.sort(cmp=lambda x,y : cmp(x[2], y[2]))
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
            kwargs['proxies'] = {"http": "http://%s:%s" % (proxy[0], proxy[1])} if not 'proxies' in kwargs else kwargs['proxies']
            kwargs['timeout'] = CONS.PROXYTIMEOUT if kwargs.get('timeout') is None else max(kwargs['timeout'], CONS.PROXYTIMEOUT)
        return fun(*args, **kwargs)
    return wrapper

def getHtmlNodeContent(node, consrc, *args, **kwargs):
    """
        获取html的节点属性内容或者内容，从内容的节点的父辈节点可以获取
        @param node: html节点
        @param consrc: 'TEXT' ｜ {'ATTR':'id' | 'name' | 'src'...}
        @param *args: 'table' | 'div' | 'ul' | 'tr'... (可选)
        @param **kwargs: attrs={'id':'A', 'class':'B', 'style':'C'...} (可选)
        @return: 节点属性内容或者str内容
    """
    if node is not None:
        if consrc == 'TEXT':
            if not args and not kwargs:
                retvar = node.text_content() or ''
            else:
                epath = './/' + args[0]
                for key, val in kwargs.items():
                    epath = epath + '[@' + key + '="' + val + '"]'
                retvar = node.find(epath)
                retvar =  retvar.text_content() if retvar is not None and retvar.text_content() is not None else ''
        else:
            if not args and not kwargs:
                retvar = node.get(consrc['ATTR']) or ''
            else:
                epath = './/' + args[0]
                for key, val in kwargs.items():
                    epath = epath + '[@' + key + '="' + val + '"]'
                retvar = node.find(epath)
                retvar =  retvar.get(consrc['ATTR']) if retvar is not None and retvar.get(consrc['ATTR']) is not None else ''
        retvar = retvar.encode('utf-8')
    else:
        retvar = ''
    return retvar.strip()

def getXmlNodeContent(node, consrc, *args, **kwargs):
    """
        获取xml的节点属性内容或者内容，从内容的节点的父辈节点可以获取
        @param node: xml节点
        @param consrc: 'TEXT' ｜ {'ATTR':'id' | 'name' | 'src'...}
        @param *args: 'table' | 'div' | 'ul' | 'tr'... (可选)
        @param **kwargs: attrs={'id':'A', 'class':'B', 'style':'C'...} (可选)
        @return: 节点属性内容或者str内容
    """
    if node is not None:
        if consrc == 'TEXT':
            if not args and not kwargs:
                retvar = node.text or ''
            else:
                epath = './/' + args[0]
                for key, val in kwargs.items():
                    epath = epath + '[@' + key + '="' + val + '"]'
                retvar = node.find(epath)
                retvar =  retvar.text if retvar is not None and retvar.text is not None else ''
        else:
            if not args and not kwargs:
                retvar = node.get(consrc['ATTR']) or ''
            else:
                epath = './/' + args[0]
                for key, val in kwargs.items():
                    epath = epath + '[@' + key + '="' + val + '"]'
                retvar = node.find(epath)
                retvar =  retvar.get(consrc['ATTR']) if retvar is not None and retvar.get(consrc['ATTR']) is not None else ''
        # retvar = retvar.encode('utf-8')
    else:
        retvar = ''
    return retvar.strip()

@byProxys
def respGet(url, headers=None, cookies=None, proxies=None, timeout=10, allow_redirects=True, format='ORIGIN', coding='utf-8', dirtys=[], myfilter=contentFilter, tofile=None, s=None):
    """
        Get方式获取页面内容，默认HTML解析，解析方式可选
        @param url: get请求目标内容地址
        @param headers: get请求头 (可选)
        @param cookies: get请求cookie (可选)
        @param proxies: get请求是否使用代理 (可选)
        @param timeout: get请求超时时间 (可选)
        @param allow_redirects: get请求是否允许重定向 (可选)
        @param format: 定义返回内容的格式，支持html-lxml.html、xml-lxml.etree、json、text (可选)
        @param coding: 解析编码处理
        @param dirtys: 抓取内容清洗
        @param tofile: 文件输出位置
        @param s: 请求会话
        @return: 返回get请求内容
    """
    if s is None:
        r = requests.get(url, headers=headers, cookies=cookies, proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    else:
        r = s.get(url, headers=headers, cookies=cookies, proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    code = r.status_code
    contents = r.content
    for one in dirtys:
        contents = contents.replace(one[0], one[1])
    contents = myfilter(contents)
    if not code in [200, 301, 302]:
        raise URLFailureException(url, code)
    contents = contents.decode(coding, 'ignore')
    if format == 'HTML':
        content = HT.fromstring(contents)
    elif format == 'JSON':
        content = unicode2utf8(json.loads(contents))
    elif format == 'XML':
        content = ET.fromstring(contents.encode('utf-8'))
    elif format == 'TEXT':
        content = contents.encode('utf-8')
    elif format == 'ORIGIN':
        content = r
    else:
        raise
    if CONS.FILEMAKE and tofile is not None:
        fi = open(CONS.FILEDIR + tofile, 'w')
        fi.write(contents)
        fi.close()
    return content

@byProxys
def respPost(url, data, headers=None, cookies=None, proxies=None, timeout=10, allow_redirects=True, format='ORIGIN', coding='utf-8', dirtys=[], myfilter=contentFilter, tofile=None, s=None):
    """
        Post方式获取页面内容，默认HTML解析，解析方式可选
        @param url: post请求目标内容地址
        @param data: post请求提交内容
        @param headers: post请求头 (可选)
        @param cookies: post请求cookie (可选)
        @param proxies: post请求是否使用代理 (可选)
        @param timeout: post请求超时时间 (可选)
        @param allow_redirects: get请求是否允许重定向 (可选)
        @param format: 定义返回内容的格式，支持html-lxml.html、xml-lxml.etree、json、text (可选)
        @param coding: 解析编码处理
        @param dirtys: 抓取内容清洗
        @param tofile: 文件输出位置
        @param s: 请求会话
        @return: 返回post请求内容
    """
    if s is None:
        r = requests.post(url, data=data, headers=headers, cookies=cookies, proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    else:
        r = s.post(url, data=data, headers=headers, cookies=cookies, proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    code = r.status_code
    contents = r.content
    for one in dirtys:
        contents = contents.replace(one[0], one[1])
    contents = myfilter(contents)
    if not code in [200, 301, 302]:
        raise 'view failure.'
    contents = contents.decode(coding, 'ignore')
    if format == 'HTML':
        content = HT.fromstring(contents)
    elif format == 'JSON':
        content = unicode2utf8(json.loads(contents))
    elif format == 'XML':
        content = ET.fromstring(contents.encode('utf-8'))
    elif format == 'TEXT':
        content = contents.encode('utf-8')
    elif format == 'ORIGIN':
        content = r
    else:
        raise 'bad request.'
    if CONS.FILEMAKE and tofile is not None:
        fi = open(CONS.FILEDIR + tofile, 'w')
        fi.write(contents)
        fi.close()
    return content

@byProxys
def respHead(url, headers=None, cookies=None, proxies=None, timeout=10, allow_redirects=True, format='ORIGIN', coding='utf-8', dirtys=[], myfilter=contentFilter, tofile=None, s=None):
    """
        Head方式获取响应状态信息
        @param url: head请求目标内容地址
        @param headers: head请求头 (可选)
        @param cookies: head请求cookie (可选)
        @param proxies: head请求是否使用代理 (可选)
        @param timeout: head请求超时时间 (可选)
        @param allow_redirects: get请求是否允许重定向 (可选)
        @param format: 定义返回内容的格式，支持html-lxml.html、xml-lxml.etree、json、text (可选)
        @param coding: 解析编码处理
        @param dirtys: 抓取内容清洗
        @param tofile: 文件输出位置
        @return: 返回head请求内容
    """
    if s is None:
        r = requests.head(url, data=data, headers=headers, cookies=cookies, proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    else:
        r = s.head(url, data=data, headers=headers, cookies=cookies, proxies=proxies, timeout=timeout, allow_redirects=allow_redirects)
    code = r.status_code
    contents = r.content
    for one in dirtys:
        contents = contents.replace(one[0], one[1])
    contents = myfilter(contents)
    if not code in [200, 301, 302]:
        raise 'view failure.'
    contents = contents.decode(coding, 'ignore')
    if format == 'HTML':
        content = HT.fromstring(contents)
    elif format == 'JSON':
        content = unicode2utf8(json.loads(contents))
    elif format == 'XML':
        content = ET.fromstring(contents.encode('utf-8'))
    elif format == 'TEXT':
        content = contents.encode('utf-8')
    elif format == 'ORIGIN':
        content = r
    else:
        raise 'bad request.'
    if CONS.FILEMAKE and tofile is not None:
        fi = open(CONS.FILEDIR + tofile, 'w')
        fi.write(contents)
        fi.close()
    return content

def respImg(url, tofile=None):
    """
        获取在线图片
        @param url: 请求图片地址
        @param tofile: 图片文件输出位置
        @return: 返回图片对象
    """
    r = urllib2.Request(url)
    img_data = urllib2.urlopen(r).read()
    img_buffer = StringIO.StringIO(img_data)
    img = Image.open(img_buffer)
    if CONS.FILEMAKE and tofile is not None:
        img.save(CONS.FILEDIR + tofile)
    return img

def treeHtml(content, coding='unicode'):
    """
        解析HTML
        @param content: Html内容
        @return: Html dom树
    """
    if coding is None or coding == 'unicode':
        pass
    else:
        content = content.decode(coding, 'ignore')
    return HT.fromstring(content)

def treeXml(content, coding='unicode'):
    """
        解析XML
        @param content: Xml内容
        @return: Xml dom树
    """
    if coding is None or coding == 'unicode':
        pass
    else:
        content = content.decode(coding, 'ignore')
    return ET.fromstring(content)

if __name__ == '__main__':
    print 'start...'
    print 'hkhkh', respHead('http://www.homeinns.com/hotel/027060')
    print 'end...'