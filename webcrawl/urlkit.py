
import urlparse

def parturl(url):
    queryparas = dict(urlparse.parse_qsl(urlparse.urlparse(url).query))
    routeparas = url.split('//')[-1]
    routeparas = routeparas[routeparas.index('/')+1:]
    routeparas = routeparas.split('?')[0]
    routeparas = tuple(routeparas.split('/'))
    return routeparas, queryparas

def urljoin(refurl, objurl):
    """
    >>> urljoin('http://www.homeinns.com/hotel', 'http://www.homeinns.com/beijing')
    'http://www.homeinns.com/beijing'
    >>> urljoin('http://www.homeinns.com/hotel', '/beijing')
    'http://www.homeinns.com/beijing'
    >>> urljoin('http://www.homeinns.com/hotel', 'beijing')
    'http://www.homeinns.com/beijing'
    """
    if objurl.strip() in ('', '#'):
        return ''
    elif objurl.startswith('http'):
        return objurl
    elif objurl.startswith('/'):
        refurl = refurl.replace('//', '{$$}')
        return ''.join([refurl[:refurl.index('/')].replace('{$$}', '//'), objurl])
    else:
        return '/'.join([refurl[:refurl.rindex('/')], objurl])

class URLParse(object):

    @classmethod
    def decode(cls, url):
        urlobj = urlparse.urlparse(url)
        params_dict = urlparse.parse_qs(urlobj.query, keep_blank_values=1)
        for key in params_dict:
            if len(params_dict[key]) == 1:
                params_dict[key] = params_dict[key][0]
        return urlobj, params_dict

    @classmethod
    def encode(cls, urlobj, params_dict):
        params_list = []
        for key in params_dict:
            if type(params_dict[key]) == list:
                for val in params_dict[key]:
                    params_list.append((key, val))
            else:
                params_list.append((key, params_dict[key]))

        query = '&'.join(['%s=%s' % (k, v) for k, v in params_list])
        urlobj = urlparse.ParseResult(scheme=urlobj.scheme, 
            netloc=urlobj.netloc, 
            path=urlobj.path, 
            params=urlobj.params, 
            query=query, 
            fragment=urlobj.fragment)
        return urlparse.urlunparse(urlobj)


if __name__ == '__main__':
    pass

