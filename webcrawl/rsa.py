#!/usr/bin/env python
# coding=utf-8

import copy, math

def charToHex(n):
    t = 48
    u = t + 9
    i = 97
    f = i + 25
    r = 65
    return n - t if n >= t and n <= u else (10 + n - r if n >= r and n <= 90 else (10 + n - i if n >= i and n <= f else 0))

def hexToDigit(hexstr):
    digit = 0
    for k in range(0, min(len(hexstr), 4)):
        digit <<= 4
        digit |= charToHex(ord(hexstr[k]))
    return digit

def arrayCopy(n, t, i, r, u):
    o = min(t + u, len(n))
    f = t
    e = r
    while f < o:
        i[e] = n[f]
        f = f + 1
        e = e + 1

class RSA(object):
    def __init__(self):
        self.maxDigits = 131
        self.zeros = [0] * self.maxDigits
        self.bigZero = self.bigInt(self.maxDigits)
        self.bigOne = self.bigInt(self.maxDigits)
        self.bigOne['digits'][0] = 1
        self.biRadixBits = 16
        self.bitsPerDigit = 16
        self.biRadix = 65536
        self.maxDigitVal = self.biRadix - 1
        self.biRadixSquared = self.biRadix * self.biRadix
        self.biHalfRadix = self.biRadix >> 1
        self.hexatrigesimalToChar = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"];
        self.lowBitMasks = [0, 1, 3, 7, 15, 31, 63, 127, 255, 511, 1023, 2047, 4095, 8191, 16383, 32767, 65535]
        self.highBitMasks = [0, 32768, 49152, 57344, 61440, 63488, 64512, 65024, 65280, 65408, 65472, 65504, 65520, 65528, 65532, 65534, 65535]
        self.hexToChar = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"]

    def biFromHex(self, hexstr):
        big = self.bigInt(self.maxDigits)
        skip = len(hexstr)
        index = 0
        while skip>0:
            big['digits'][index] = hexToDigit(hexstr[max(skip-4, 0):max(skip-4, 0)+min(skip, 4)])
            skip = skip - 4
            index = index + 1
        return big

    def biHighIndex(self, big):
        skip = len(big['digits']) - 1
        while skip>0 and big['digits'][skip] == 0:
            skip = skip - 1
        return skip

    def digitToHex(self, n):
        t = "";
        for i in range(0, 4):
            t += self.hexToChar[n&15]
            n >>= 4
        t = t[::-1]
        return t

    def biNumBits(self, big):
        k = self.biHighIndex(big)
        flag = big['digits'][k]
        limit = (k + 1) * self.bitsPerDigit
        skip = limit
        while skip > limit - self.bitsPerDigit:
            if not (flag & 32768) == 0:
                break
            flag <<= 1
            skip = skip - 1
        return skip

    def bigInt(self, obj):
        big = {}
        big['digits'] = None if type(obj) == bool and obj == True else copy.deepcopy(self.zeros)
        big['isNeg'] = False
        return big

    def biShiftRight(self, n, t):
        e = int(t / float(self.bitsPerDigit))
        i = self.bigInt(self.maxDigits)
        arrayCopy(n['digits'], e, i['digits'], 0, len(n['digits']) - e)
        u = t % self.bitsPerDigit
        o = self.bitsPerDigit - u
        r = 0
        f = r + 1
        while r < len(i['digits'])-1:
            i['digits'][r] = i['digits'][r] >> u | (i['digits'][f] & self.lowBitMasks[u]) << o
            r = r + 1
            f = f + 1
        i['digits'][len(i['digits']) - 1] >>= u
        i['isNeg'] = n['isNeg']
        return i

    def biCopy(self, big):
        return copy.deepcopy(big)

    def biMultiplyByRadixPower(self, n, t):
        i = self.bigInt(self.maxDigits)
        arrayCopy(n['digits'], 0, i['digits'], t, len(i['digits']) - t)
        return i

    def biSubtract(self, n, t):
        if not n['isNeg'] == t['isNeg']:
            t['isNeg'] = not t['isNeg']
            r = self.biAdd(n, t)
            t['isNeg'] = not t['isNeg']
        else:
            r = self.bigInt(self.maxDigits)
            u = 0
            for i in range(0, len(n['digits'])):
                f = n['digits'][i] - t['digits'][i] + u
                r['digits'][i] = f & self.maxDigitVal
                if r['digits'][i]<0:
                    r['digits'][i] += self.biRadix
                u = 0 - int(f < 0)
            if u == -1:
                u = 0
                for i in range(0, len(n['digits'])):
                    f = 0 - r['digits'][i] + u
                    r['digits'][i] = f & self.maxDigitVal
                    if r['digits'][i] < 0:
                        r['digits'][i] += self.biRadix
                u = 0 - int(f < 0)
                r['isNeg'] = not n['isNeg']
            else:
                r['isNeg'] = n['isNeg']
        return r

    def biAdd(self, n, t):
        if not n['isNeg'] == t['isNeg']:
            t['isNeg'] = not t['isNeg']
            r = self.biSubtract(n, t)
            t['isNeg'] = not t['isNeg'];
        else:
            r = self.bigInt(self.maxDigits)
            u = 0
            i = 0
            while i < len(n['digits']):
                f = n['digits'][i] + t['digits'][i] + u
                r['digits'][i] = f & self.maxDigitVal
                u = int(f >= self.biRadix)
                i = i + 1
            r['isNeg'] = n['isNeg']
        return r

    def biCompare(self, n, t):
        if not n['isNeg'] == t['isNeg']:
            return 1 - 2 * int(n['isNeg'])
        i = len(n['digits']) -1
        while i>=0:
            if not n['digits'][i] == t['digits'][i]:
                return 1 - 2 * int(n['digits'][i] > t['digits'][i]) if n['isNeg'] else 1 - 2 * int(n['digits'][i] < t['digits'][i])
            i = i - 1
        return 0

    def biShiftLeft(self, n, t):
        e = int(t / float(self.bitsPerDigit))
        i = self.bigInt(self.maxDigits)
        arrayCopy(n['digits'], 0, i['digits'], e, len(i['digits']) - e)
        u = t % self.bitsPerDigit
        o = self.bitsPerDigit - u
        r = len(i['digits']) - 1
        f = r - 1
        while r > 0:
            i['digits'][r] = i['digits'][r] << u & self.maxDigitVal | (i['digits'][f] & self.highBitMasks[u]) >> o
            r = r - 1
            f = f -1
        i['digits'][0] = i['digits'][r] << u & self.maxDigitVal
        i['isNeg'] = n['isNeg']
        return i

    def biMultiplyDigit(self, n, t):
        result = self.bigInt(self.maxDigits)
        u = self.biHighIndex(n)
        r = 0
        for i in range(0, u + 1):
            f = result['digits'][i] + n['digits'][i] * t + r
            result['digits'][i] = f & self.maxDigitVal
            r = f >> self.biRadixBits
        result['digits'][1 + u] = r
        return result

    def biDivideModulo(self, n, t):
        a = self.biNumBits(n)
        s = self.biNumBits(t)
        v = t['isNeg']
        if a < s:
            if n['isNeg']:
                r = copy.deepcopy(self.bigOne)
                r['isNeg'] = not t['isNeg']
                n['isNeg'] = False
                t['isNeg'] = False
                i = self.biSubtract(t, n)
                n['isNeg'] = True
                t['isNeg'] = v
            else:
                r = self.bigInt(self.maxDigits)
                i = copy.deepcopy(n)
            return (r, i)
        r = self.bigInt(self.maxDigits)
        u = int(math.ceil((s / float(self.bitsPerDigit))) - 1)
        e = 0
        i = n
        while t['digits'][u] < self.biHalfRadix:
            t = self.biShiftLeft(t, 1)
            e = e + 1
            s = s + 1
            u = int(math.ceil((s / float(self.bitsPerDigit))) - 1)
        i = self.biShiftLeft(i, e)
        a = a + e
        h = int(math.ceil((a / float(self.bitsPerDigit))) - 1)
        o = self.biMultiplyByRadixPower(t, h - u)
        while not self.biCompare(i, o) == -1:
            r['digits'][h - u] = r['digits'][h - u] + 1
            i = self.biSubtract(i, o)
        f = h
        while f > u:
            c = 0 if f >= len(i['digits']) else i['digits'][f]
            y = 0 if f - 1 >= len(i['digits']) else i['digits'][f-1]
            b = 0 if f - 2 >= len(i['digits']) else i['digits'][f-2]
            l = 0 if u >= len(t['digits']) else t['digits'][u]
            k = 0 if u - 1 >= len(t['digits']) else t['digits'][u-1]
            r['digits'][f - u - 1] = self.maxDigitVal if c == l else int((c * self.biRadix + y) / float(l))
            p = r['digits'][f - u - 1] * (l * self.biRadix + k)
            w = c * self.biRadixSquared + (y * self.biRadix + b)
            while p > w:
                r['digits'][f - u - 1] = r['digits'][f - u - 1] -1
                p = r['digits'][f - u - 1] * (l * self.biRadix | k)
                w = c * self.biRadix * self.biRadix + (y * self.biRadix + b)
            o = self.biMultiplyByRadixPower(t, f - u - 1)
            i = self.biSubtract(i, self.biMultiplyDigit(o, r['digits'][f - u - 1]))
            if i['isNeg']:
                i = self.biAdd(i, o)
                r['digits'][f - u - 1] = r['digits'][f - u - 1] - 1
            f = f - 1
        i = self.biShiftRight(i, e)
        r['isNeg'] = not n['isNeg'] == v
        if n['isNeg']:
            r = self.biAdd(r, self.bigOne) if v else self.biSubtract(r, self.bigOne)
            t = self.biShiftRight(t, e)
            i = self.biSubtract(t, i)
        if i['digits'][0] == 0 and self.biHighIndex(i) ==0:
            i['isNeg'] = False
        return (r, i)

    def biDivide(self, n, t):
        return self.biDivideModulo(n, t)[0]

    def biDivideByRadixPower(self, n, t):
        i = self.bigInt(self.maxDigits)
        arrayCopy(n['digits'], t, i['digits'], 0, len(i['digits']) - t)
        return i

    def biMultiply(self, n, t):
        r = self.bigInt(self.maxDigits)
        o = self.biHighIndex(n)
        s = self.biHighIndex(t)
        s = 64
        for i in range(0, s+1):
            u = 0
            f = i
            for j in range(0, o+1):
                e = r['digits'][f] + n['digits'][j] * t['digits'][i] + u
                r['digits'][f] = e & self.maxDigitVal
                u = e >> self.biRadixBits
                f = f + 1
            r['digits'][i + o + 1] = u
        r['isNeg'] = not n['isNeg'] == t['isNeg']
        return r

    def biModuloByRadixPower(self, n, t):
        i = self.bigInt(self.maxDigits)
        arrayCopy(n['digits'], 0, i['digits'], 0, t)
        return i

    def BarrettMu_modulo(self, m, n):
        r = self.biDivideByRadixPower(n, m['k'] - 1)
        u = self.biMultiply(r, m['mu'])
        f = self.biDivideByRadixPower(u, m['k'] + 1)
        e = self.biModuloByRadixPower(n, m['k'] + 1)
        o = self.biMultiply(f, m['modulus'])
        s = self.biModuloByRadixPower(o, m['k'] + 1)
        t = self.biSubtract(e, s)
        if t['isNeg']:
            t = self.biAdd(t, m['bkplus1'])
        i = self.biCompare(t, m['modulus']) >= 0
        while i:
            t = self.biSubtract(t, m['modulus'])
            i = self.biCompare(t, m['modulus']) >= 0
        return t

    def BarrettMu_multiplyMod(self, m, n, t):
        i = self.biMultiply(n, t)
        return m['modulo'](m, i)

    def BarrettMu_powMod(self, m, n, t):
        u = self.bigInt(self.maxDigits)
        u['digits'][0] = 1
        r = n
        i = t
        while True:
            if not i['digits'][0] & 1 == 0:
                u = m['multiplyMod'](m, u, r)
            i = self.biShiftRight(i, 1)
            if i['digits'][0] == 0 and self.biHighIndex(i) == 0:
                break
            r = m['multiplyMod'](m, r, r)
        return u

    def barrettMu(self, big):
        barretmu = {}
        barretmu['modulus'] = copy.deepcopy(big)
        barretmu['k'] = self.biHighIndex(barretmu['modulus']) + 1
        anobig = self.bigInt(self.maxDigits)
        anobig['digits'][2*barretmu['k']] = 1
        barretmu['mu'] = self.biDivide(anobig, barretmu['modulus'])
        barretmu['bkplus1'] = self.bigInt(self.maxDigits)
        barretmu['bkplus1']['digits'][barretmu['k']+1] = 1
        barretmu['modulo'] = self.BarrettMu_modulo
        barretmu['multiplyMod'] = self.BarrettMu_multiplyMod
        barretmu['powMod'] = self.BarrettMu_powMod
        return barretmu

    def biToHex(self, n):
        i = ""
        # r = self.biHighIndex(n)
        t = self.biHighIndex(n)
        while t > -1:
            i += self.digitToHex(n['digits'][t])
            t = t - 1
        return i

    def biToString(self, n, t):
        r = self.bigInt(self.maxDigits)
        r['digits'][0] = t
        i = self.biDivideModulo(n, r)
        u = self.hexatrigesimalToChar[i[1]['digits'][0]]
        while self.biCompare(i[0], self.bigZero) == 1:
            i = self.biDivideModulo(i[0], r)
            # digit = i[1]['digits'][0]
            u += self.hexatrigesimalToChar[i[1]['digits'][0]]
        u = u[::-1]
        return ("-" if n['isNeg'] else "") + u

    def RSAKeyPair(self, n, t, i):
        rsa = {}
        rsa['e'] = self.biFromHex(n)
        rsa['d'] = self.biFromHex(t)
        rsa['m'] = self.biFromHex(i)
        rsa['digitSize'] = 2 * self.biHighIndex(rsa['m']) + 2
        rsa['chunkSize'] = 2 * self.biHighIndex(rsa['m'])
        rsa['radix'] = 16
        rsa['barrett'] = self.barrettMu(rsa['m'])
        return rsa

    def encryptedString(self, n, t):
        pass


if __name__ == '__main__':
    pass
