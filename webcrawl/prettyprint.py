#!/usr/bin/python
# coding=utf-8

"""
   traceback message or other files
"""
import sys
import os
import datetime
import logging
import traceback
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

from . import MyLocal
from character import _cs, _cu

CFG = MyLocal(debug=True, dir=os.path.expanduser('~/log/'), handlers=[], exist=[])


def _print(*args):
    """
        Print txt by coding GBK.

        *args
          list, list of printing contents
        
    """
    if not CFG.debug:
        return
    if not args:
        return
    encoding = 'gbk'
    args = [_cs(a, encoding) for a in args]
    f_back = None
    try:
        raise Exception
    except:
        f_back = sys.exc_traceback.tb_frame.f_back
    f_name = f_back.f_code.co_name
    filename = os.path.basename(f_back.f_code.co_filename)
    m_name = os.path.splitext(filename)[0]
    prefix = ('[%s.%s]'%(m_name, f_name)).ljust(20, ' ')
    if os.name == 'nt':
        for i in range(len(args)):
            v = args [i]
            if isinstance(v, str):
                args[i] = v #v.decode('utf8').encode('gbk')
            elif isinstance(v, unicode):
                args[i] = v.encode('gbk')
    print '[%s]'%str(datetime.datetime.now()), prefix, ' '.join(args)


def _print_err(*args):
    """
        Print errors.

        *args
          list, list of printing contents
        
    """
    if not CFG.debug:
        return
    if not args:
        return
    encoding = 'utf8' if os.name == 'posix' else 'gbk'
    args = [_cs(a, encoding) for a in args]
    f_back = None
    try:
        raise Exception
    except:
        f_back = sys.exc_traceback.tb_frame.f_back
    f_name = f_back.f_code.co_name
    filename = os.path.basename(f_back.f_code.co_filename)
    m_name = os.path.splitext(filename)[0]
    prefix = ('[%s.%s]'%(m_name, f_name)).ljust(20, ' ')
    print bcolors.FAIL+'[%s]'%str(datetime.datetime.now()), prefix, ' '.join(args) + bcolors.ENDC


def logprint(logname, category, level='INFO', backupCount=15):
    """
        Print logs by datetime.

        logname
          string, file name
        category
          string, category path of logs file in log directory
        level
          string, restrict whether logs to be printed or not
        backupCount
          int, how many backups can be reserved
        
    """
    path = os.path.join(CFG.dir, category.strip('/'), logname.strip('/') + '.log')
    print "log path:", path
    if not os.path.exists(path[:path.rindex('/')]):
        os.makedirs(path[:path.rindex('/')])
    # Initialize logger
    logger = logging.getLogger(logname)
    frt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    hdr = logging.StreamHandler(sys.stdout)
    hdr.setFormatter(frt)
    hdr._name = '##_sh_##'
    if not hdr._name in CFG.exist:
        logger.addHandler(hdr)
        CFG.exist.append(hdr._name)

    hdr = TimedRotatingFileHandler(path, 'D', 1, backupCount)
    hdr.setFormatter(frt)
    hdr._name = '##_rfh_##'
    if not hdr._name in CFG.exist:
        logger.addHandler(hdr)
        CFG.exist.append(hdr._name)


    if level.upper() == 'NOTEST':
        level == logging.NOTSET
    elif level.upper() == 'DEBUG':
        level == logging.DEBUG
    elif level.upper() == 'WARNING':
        level == logging.WARNING
    elif level.upper() == 'ERROR':
        level == logging.ERROR
    elif level.upper() == 'CRITICAL':
        level == logging.CRITICAL
    else:
        level == logging.INFO
    logger.setLevel(level)

    def _wraper(*args, **kwargs):
        if not CFG.debug:
            return
        if not args:
            return
            
        for hdr in CFG.handlers:
            if not hdr._name in CFG.exist:
                logger.addHandler(hdr)
                CFG.exist.append(hdr._name)

        encoding = 'utf8' if os.name == 'posix' else 'gbk'
        args = [_cu(a, encoding) for a in args]

        prefix = ''

        pl = kwargs.get('printlevel', 'info').upper()
        if pl == 'DEBUG':
            try:
                logger.debug(*args, **kwargs)
            except:
                t, v, b = sys.exc_info()
                err_messages = traceback.format_exception(t, v, b)
                print 'Error: %s' % ','.join(err_messages)
        elif pl == 'WARNING':
            try:
                logger.warning(*args, **kwargs)
            except:
                t, v, b = sys.exc_info()
                err_messages = traceback.format_exception(t, v, b)
                print 'Error: %s' % ','.join(err_messages)
        elif pl == 'ERROR':
            try:
                logger.error(*args, **kwargs)
            except:
                t, v, b = sys.exc_info()
                err_messages = traceback.format_exception(t, v, b)
                print 'Error: %s' % ','.join(err_messages)
        elif pl == 'CRITICAL':
            try:
                logger.critical(*args, **kwargs)
            except:
                t, v, b = sys.exc_info()
                err_messages = traceback.format_exception(t, v, b)
                print 'Error: %s' % ','.join(err_messages)
        else:
            try:
                logger.info(*args, **kwargs)
            except:
                t, v, b = sys.exc_info()
                err_messages = traceback.format_exception(t, v, b)
                print 'Error: %s' % ','.join(err_messages)
    return _wraper, logger


def fileprint(filename, category, level=logging.DEBUG, maxBytes=1024*10124*100,
             backupCount=0):
    """
        Print files by file size.

        filename
          string, file name
        category
          string, category path of logs file in log directory
        level
          enumerated type of logging module, restrict whether logs to be printed or not
        maxBytes
          int, max limit of file size
        backupCount
          int, allowed numbers of file copys
        
    """
    path = os.path.join(CFG.filedir, category, filename)

    # Initialize filer
    filer = logging.getLogger(filename)
    frt = logging.Formatter('%(message)s')

    hdr = RotatingFileHandler(path, 'a', maxBytes, backupCount, 'utf-8')
    hdr.setFormatter(frt)
    hdr._name = '##_rfh_##'
    already_in = False
    for _hdr in filer.handlers:
        if _hdr._name == '##_rfh_##':
            already_in = True
            break
    if not already_in:
        filer.addHandler(hdr)

    hdr = logging.StreamHandler(sys.stdout)
    hdr.setFormatter(frt)
    hdr._name = '##_sh_##'
    already_in = False
    for _hdr in filer.handlers:
        if _hdr._name == '##_sh_##':
            already_in = True
    if not already_in:
        filer.addHandler(hdr)

    filer.setLevel(level)
    def _wraper(*args):
        if not args:
            return
        encoding = 'utf8' if os.name == 'posix' else 'gbk'
        args = [_cu(a, encoding) for a in args]
        filer.info(' '.join(args))
    return _wraper, filer

class bcolors:
    """
        定义常用颜色
    """
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

if __name__ == '__main__':
    pass
