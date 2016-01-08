#!/usr/bin/python
# coding=utf8

"""
    安装包工具
"""

import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()

requires = [
    'requests>=2.2.1',
    'lxml>=3.3.2',
    'gitpython', # 0.3.2.RC1
    'gitdb', # 0.5.4
    'chardet',
    'PIL',
    'gevent',
    'beanstalkc',
    ]

webcrawl = __import__('webcrawl')
setup(name='webcrawl',
version=webcrawl.__version__,
description='wecatch webcrawl',
long_description='',
author='haokuan',
author_email='jingdaohao@gmail.com',
url='http://www.google.com',
keywords='wecatch > ',
packages=find_packages(),
# package_data={'':['*.js', '*.css']},
namespace_packages=['webcrawl',],
include_package_data=True,
zip_safe=False,
install_requires=requires,
entry_points="",
scripts=['bin/browse'],
)

