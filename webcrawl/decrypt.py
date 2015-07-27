#!/usr/bin/python
# coding=utf-8

"""
   酒店相关解密
"""


def hextoasiitochar(hexstr):
    """
        将四位十六进制转换成asii码，再转换成字符
        @param hexstr: 4位16进制字符串
        @return: 单个字符
    """
    hexstr = hexstr.upper()
    basestr = '0123456789ABCDEF'
    basenum = {'0': 0, '1': 1, '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7,
               '8': 8, '9': 9, 'A': 10, 'B': 11, 'C': 12, 'D': 13, 'E': 14, 'F': 15}
    assert type(hexstr) == str and len(hexstr) == 4
    for c in hexstr:
        if not c in basestr:
            raise "Wrong hexadecimal number string."
    asiinum = 0
    hexstr = hexstr[::-1]
    for c in hexstr:
        asiinum += basenum[c] * (16 ** hexstr.index(c))
    return chr(asiinum)


def decrypt100innhid(ehid):
    """
        易佰的酒店ID的解密
        @param ehid: 解密前的易佰酒店ID
        @return hid: 解密后的易佰酒店ID
    """
    assert type(ehid) == str and len(ehid) % 4 == 0
    index = 0
    hid = ''
    for fourhex in ehid[::4]:
        fourhex = ehid[index:index + 4]
        index = index + 4
        hid += hextoasiitochar(fourhex)
    return hid

if __name__ == '__main__':
    print 'start...'
    print hextoasiitochar('006E')
    print decrypt100innhid('0063006E00730068007300680079003100300032')
    print 'end...'
