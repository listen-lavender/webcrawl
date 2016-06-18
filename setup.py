#!/usr/bin/env python
# coding=utf8

"""
    安装包工具
"""

import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()

install_requires = [
    'requests>=2.2.1',
    'lxml>=3.3.2',
    'gitpython', # 0.3.2.RC1
    'gitdb', # 0.5.4
    'chardet',
    'gevent',
    'pycrypto',
    'redis',
    'beanstalkc'
    ]

webcrawl = __import__('webcrawl')
setup(name='webcrawl',
version=webcrawl.__version__,
description='wecatch webcrawl',
author='haokuan',
author_email='jingdaohao@gmail.com',
url='https://github.com/listen-lavender/webcrawl',
keywords='wecatch > ',
packages=find_packages(),
install_requires=install_requires,
scripts=['bin/browse'],
)

