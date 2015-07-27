#!/usr/bin/python
# coding=utf-8

"""
   映射关系
"""
import os

SRCPKGPATH = os.path.expanduser('~') + '/tmp/python/'
DESCPKGPATH = '/home/sysmon/PythonPackages'

EXCEPTS = ['pkg_resources', 'Queue', 'fcntl', 'functools', 'random', 'datetime', 'unittest', 'string', 're', 'json', 'collections', 'smtplib',
           'new', 'math', 'stat', 'urllib2', 'sys', 'copy', 'types', 'hashlib', 'logging', 'StringIO', 'traceback', 'threading', 'time', 'os']
TRANS = {'MySQLdb': 'mysql-python', 'git': "gitpython','gitdb", 'Image': 'PIL'}

defaultInit = """#!/usr/bin/python
# coding=utf-8
"""

topInit = """#!/usr/bin/python
# coding=utf8

'''
    命名空间的内容初始化
'''

__import__('pkg_resources').declare_namespace(__name__)
__version__ = {#version#}
__author__ = '@fasthotel'
"""

spaceTXT = """{'import':{#import#},
        'name':{#name#},
        'description':'',
        'author':'@fasthotel',
        'author_email':'kuaijie@innapp.cn',
        'url':'http://h.133.cn/hw/',
        'keywords':'fasthotel > ',
        'namespace_packages':{#namespace_packages#},
        'install_requires':{#install_requires#},
        'scripts':{#scripts#},
        }
"""

setupTXT = """#!/usr/bin/python
# coding=utf8

'''
    安装包工具
'''

# import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
CHANGES = open(os.path.join(here, 'CHANGES.rst')).read()

sps = [{#sps#}]

for one in sps:
    hk = __import__(one['import'])
    setup(name=one['name'],
    version=hk.__version__,
    description=one['description'],
    long_description='',
    author=one['author'],
    author_email=one['author_email'],
    url=one['url'],
    keywords=one['keywords'],
    packages=find_packages(),
    namespace_packages=one['namespace_packages'],
    include_package_data=True,
    zip_safe=False,
    install_requires=one['install_requires'],
    entry_points="",
    scripts=one['scripts'],
    )
"""

manifestTXT = """recursive-include {#name#}/ *
recursive-exclude {#name#}/ *.pyc
include README.rst
include README.md
include CHANGES.rst
"""
readmeTXT = """ This is {#name#}
"""
changeTXT = """ Package {#name#} change to {#version#}
"""


def makeAssistdoc(pkgpath, pkgname, version):
    if os.path.exists(pkgpath + 'MANIFEST.in'):
        pass
    else:
        manifestdatas = manifestTXT.replace('{#name#}', pkgname)
        fi = open(pkgpath + 'MANIFEST.in', 'w')
        fi.write(manifestdatas)
        fi.close()
    if os.path.exists(pkgpath + 'README.rst'):
        pass
    else:
        readmedatas = readmeTXT.replace('{#name#}', pkgname)
        fi = open(pkgpath + 'README.rst', 'w')
        fi.write(readmedatas)
        fi.close()
        fi = open(pkgpath + 'README.md', 'w')
        fi.write(readmedatas)
        fi.close()
    if os.path.exists(pkgpath + 'CHANGES.rst'):
        pass
    else:
        changedatas = changeTXT.replace('{#name#}', pkgname)
        fi = open(pkgpath + 'CHANGES.rst', 'w')
        fi.write(changedatas)
        fi.close()


def checkVersionstr(version):
    """
        检查版本号是否规范
        @param version: 版本号
        @return : 检查状态
    """
    if version is None:
        return False
    else:
        return True


def pickupRely(filepath):
    """
        提取python文件的依赖
        @param filepath: python文件地址
        @return relys: 依赖列表
    """
    relys = []
    for line in open(filepath):
        line = line.strip()
        if line.startswith('#'):
            continue
        for part in line.split(';'):
            if 'from ' in part and 'import ' in part:
                mod = part[
                    part.index('from ') + len('from '):part.index('import')].strip() + '.'
                relys.append(mod[:mod.index('.')])
                if mod[:mod.index('.')] == 'cada':
                    print filepath
            elif '__import__(' in part:
                mod = part[part.index(
                    '__import__(') + len('__import__('):].replace("'", '').replace(')', '').strip() + '.'
                relys.append(mod[:mod.index('.')])
                if mod[:mod.index('.')] == 'cada':
                    print filepath
            elif 'import ' in part:
                for mod in part[part.index('import ') + len('import '):].split(','):
                    mod = mod.strip() + '.'
                    relys.append(mod[:mod.index('.')])
                    if mod[:mod.index('.')] == 'cada':
                        print filepath
            else:
                pass
    relys = list(set(relys))
    return relys


def initDir(fdir, itemlist, isTop=False, version=None):
    """
        提取python文件的依赖
        @param fdir: 目录地址
        @param itemlist: 目录元素列表
        @param isTop: 是否是顶层目录
        @return version: 顶层目录文件需要的版本号
    """
    if isTop == True:
        fi = open(os.path.join(fdir, '__init__.py'), 'w')
        assert checkVersionstr(version) == True
        datas = topInit.replace('{#version#}', "'" + version + "'")
        fi.write(datas)
        fi.close()
    for item in itemlist:
        if '__init__' in item:
            break
    else:
        fi = open(os.path.join(fdir, '__init__.py'), 'w')
        datas = defaultInit
        fi.write(datas)
        fi.close()
    return True


def walkDir(fdir, version, topdown=True, pkgname=None, pkgpath=SRCPKGPATH):
    import datetime
    dt = datetime.datetime.now().strftime('%Y%m')
    relys = []
    excepts = EXCEPTS
    bins = []
    isTop = True
    assert os.path.exists(fdir), '打包源不存在'
    if pkgname is None:
        pkgname = fdir.strip('/')
        pkgname = pkgname[pkgname.rindex('/') + 1:]
        excepts.append(pkgname)
    else:
        excepts.append(pkgname)
    pfdir = fdir.strip('/')
    pfdir = '/' + pfdir[:pfdir.rindex('/')] + '/'
    pkgpath = os.path.join(pkgpath, pkgname + dt + '/')
    print 'namespace: ', fdir, 'packagespace: ', pfdir
    if os.path.exists(pkgpath):
        pass
    else:
        os.makedirs(pkgpath)
    if os.path.exists(pfdir + 'setup.py'):
        fi = open(fdir + '__init__.py', 'r')
        datas = fi.read()
        for line in fi.readlines():
            if '__version__' in line and version:
                datas = datas.replace(line, "__version__ = '%s'" % version)
                break
        else:
            datas = topInit.replace('{#version#}', "'" + version + "'")
        fi.close()
        fi = open(fdir + '__init__.py', 'w')
        fi.write(datas)
        fi.close()
        print '初始化目录'
        assert os.system("rsync -av --exclude '*.pyc' --exclude '*.log' --exclude '*.jpg' --exclude '*.png' %s %s" %
                         (pfdir + '*', pkgpath)) == 0, '初始化打包目录失败'
    else:
        print '初始化目录'
        assert os.system("rsync -av --exclude '*.pyc' --exclude '*.log' --exclude '*.jpg' --exclude '*.png' %s %s" %
                         ('/' + fdir.strip('/'), pkgpath)) == 0, '初始化打包目录失败'
        fdir = pkgpath + pkgname
        print fdir
        for root, dirs, files in os.walk(fdir, topdown):
            if root.endswith('/bin'):
                bins.extend([(root + '/' + fi).replace(pkgpath, '')
                             for fi in files])
                continue
            for filename in files:
                if filename.endswith('.py'):
                    excepts.append(filename.replace('.py', ''))
                    relys.extend(pickupRely(os.path.join(root, filename)))
                    break
            else:
                if len(files) > 0:
                    continue
            if isTop:
                initDir(root, files, isTop=isTop, version=version)
                isTop = False
            else:
                initDir(root, files)
            excepts.extend(dirs)
        relys = list(set(relys).difference(set(excepts)))
        relys = "".join(("['", "','".join(relys), "']"))
        for key, val in TRANS.items():
            relys = relys.replace(key, val)
        namespace_packages = "['" + pkgname + "',]"
        if len(bins) > 0:
            bins = ''.join(("['", "','".join(bins), "']"))
        else:
            bins = '[]'
        setupdatas = setupTXT.replace('{#sps#}', spaceTXT.replace('{#import#}', "'" + pkgname + "'").replace('{#name#}', "'" + pkgname + "'").replace(
            '{#namespace_packages#}', namespace_packages).replace('{#install_requires#}', relys).replace('{#scripts#}', bins))
        fi = open(pkgpath + 'setup.py', 'w')
        fi.write(setupdatas)
        fi.close()
    makeAssistdoc(pkgpath, pkgname, version)
    print 'make package path: ', pkgpath
    assert os.system('&&'.join(('. ~/.bash_profile', 'easy_install -mxN %s' % pkgname,
                                'cd %s' % pkgpath, 'python setup.py install', 'python setup.py sdist'))) == 0, '打包失败'
    print '打包成功'
    pkg = None
    for it in os.listdir(pkgpath + 'dist/'):
        pkg = pkgpath + 'dist/' + it
        if os.path.isfile(pkg) and ('.tar.gz' in pkg or '.zip' in pkg) and version in pkg:
            break
    else:
        pkg = None
    assert pkg is not None, '包不存在'
    # assert os.system('cp %s %s' % (pkg, fdir)) == 0, '打包失败'
    # os.system('rm -rf %s' % pkgpath)
    return pkg, pkgpath


def flushPipindex():
    assert os.system('&&'.join(
        ('. ~/.bash_profile', 'cd %s' % DESCPKGPATH, 'dir2pi .'))) == 0, '刷新索引失败'
    print '刷新索引成功'

if __name__ == '__main__':
    print 'start...'
    fdir = '/Users/haokuan/bitbucket/tools'
    walkDir(fdir, '2.2.2')
    flushPipindex()
    print 'end...'
