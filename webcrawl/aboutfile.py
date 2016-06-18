#!/usr/bin/python
# coding=utf-8

"""
    tools of handling files
"""

import os

def modulepath(filename):
    """
    Find the relative path to its module of a python file if existing.

    filename
      string, name of a python file
    """
    filepath = os.path.abspath(filename)
    prepath = filepath[:filepath.rindex('/')]
    postpath = '/'
    if prepath.count('/') == 0 or not os.path.exists(prepath + '/__init__.py'):
        flag = False
    else:
        flag = True
    while True:
        if prepath.endswith('/lib') or prepath.endswith('/bin') or prepath.endswith('/site-packages'):
            break
        elif flag and (prepath.count('/') == 0 or not os.path.exists(prepath + '/__init__.py')):
            break
        else:
            for f in os.listdir(prepath):
                if '.py' in f:
                    break
            else:
                break
            postpath = prepath[prepath.rindex('/'):].split('-')[0].split('_')[0] + postpath
            prepath = prepath[:prepath.rindex('/')]
    return postpath.lstrip('/') + filename.split('/')[-1].replace('.pyc', '').replace('.py', '') + '/'

def modulename(filename):
    """
    Find the modulename from filename.

    filename
      string, name of a python file
    """
    return filename.split('/')[-1].replace('.pyc', '').replace('.py', '')

if __name__ == '__main__':
    print modulepath(__file__)
