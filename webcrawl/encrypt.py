#!/usr/bin/python
# coding=utf-8

"""
   酒店相关加密
"""


def chrbyasiitohex(achr):
    """
        加密易佰的酒店ID解密
        @param achr: 单个字符
        @return hexstr: 4位16进制字符串
    """
    assert type(achr) == str and len(achr) == 1
    basechr = {0: '0', 1: '1', 2: '2', 3: '3', 4: '4', 5: '5', 6: '6', 7: '7',
               8: '8', 9: '9', 10: 'A', 11: 'B', 12: 'C', 13: 'D', 14: 'E', 15: 'F'}
    asiinum = ord(achr)
    hexstr = ''
    while True:
        c = basechr[asiinum % 16]
        hexstr = c + hexstr
        asiinum = (asiinum - asiinum % 16) / 16
        if asiinum < 1:
            break
    return hexstr.rjust(4, '0')


def encrypt100innhid(dhid):
    """
        易佰的酒店ID的加密
        @param dhid: 加密前的易佰酒店ID
        @return hid: 加密后的易佰酒店ID
    """
    assert type(dhid) == str
    hid = ''
    for c in dhid:
        hid += chrbyasiitohex(c)
    return hid

if __name__ == '__main__':
    print 'start...'
    print chrbyasiitohex('c')
    print encrypt100innhid('cnshshy102')
    print 'end...'
