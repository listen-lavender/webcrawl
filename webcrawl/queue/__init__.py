#!/usr/bin/env python
# coding=utf8

import uuid
import os

def _id():
    mac = uuid.UUID(int = uuid.getnode()).hex[-12:]  
    return ":".join([mac[e:e+2] for e in range(0,11,2)])

MACADDRESS = _id()
CURRPID = os.getpid()

def fid(name):
    return '%s-%s-%s' % (MACADDRESS, CURRPID, name)