"""A cleanup tool for HTML.

Removes unwanted tags and content.  See the `HtmlCleaner` class for
details.
"""

import re
import copy
import threading
try:
    from urlparse import urlsplit
except ImportError:
    # Python 3
    from urllib.parse import urlsplit
from lxml import etree
from lxml.html import defs
from lxml.html import fromstring, tostring, XHTML_NAMESPACE
from lxml.html import xhtml_to_html, _transform_result

try:
    unichr
except NameError:
    # Python 3
    unichr = chr
try:
    unicode
except NameError:
    # Python 3
    unicode = str
try:
    bytes
except NameError:
    # Python < 2.6
    bytes = str
try:
    basestring
except NameError:
    basestring = (str, bytes)

# Look at http://code.sixapart.com/trac/livejournal/browser/trunk/cgi-bin/cleanhtml.pl
#   Particularly the CSS cleaning; most of the tag cleaning is integrated now
# I have multiple kinds of schemes searched; but should schemes be
#   whitelisted instead?
# max height?
# remove images?  Also in CSS?  background attribute?
# Some way to whitelist object, iframe, etc (e.g., if you want to
#   allow *just* embedded YouTube movies)
# Log what was deleted and why?
# style="behavior: ..." might be bad in IE?
# Should we have something for just <meta http-equiv>?  That's the worst of the
#   metas.
# UTF-7 detections?  Example:
#     <HEAD><META HTTP-EQUIV="CONTENT-TYPE" CONTENT="text/html; charset=UTF-7"> </HEAD>+ADw-SCRIPT+AD4-alert('XSS');+ADw-/SCRIPT+AD4-
#   you don't always have to have the charset set, if the page has no charset
#   and there's UTF7-like code in it.
# Look at these tests: http://htmlpurifier.org/live/smoketests/xssAttacks.php


# This is an IE-specific construct you can have in a stylesheet to
# run some Javascript:
_css_javascript_re = re.compile(
    r'expression\s*\(.*?\)', re.S|re.I)

# Do I have to worry about @\nimport?
_css_import_re = re.compile(
    r'@\s*import', re.I)

# All kinds of schemes besides just javascript: that can cause
# execution:
_is_image_dataurl = re.compile(
    r'^data:image/.+;base64', re.I).search
_is_possibly_malicious_scheme = re.compile(
    r'(?:javascript|jscript|livescript|vbscript|data|about|mocha):',
    re.I).search
def _is_javascript_scheme(s):
    if _is_image_dataurl(s):
        return None
    return _is_possibly_malicious_scheme(s)

_substitute_whitespace = re.compile(r'[\s\x00-\x08\x0B\x0C\x0E-\x19]+').sub
# FIXME: should data: be blocked?

# FIXME: check against: http://msdn2.microsoft.com/en-us/library/ms537512.aspx
_conditional_comment_re = re.compile(
    r'\[if[\s\n\r]+.*?][\s\n\r]*>', re.I|re.S)

_find_styled_elements = etree.XPath(
    "descendant-or-self::*[@style]")

_find_external_links = etree.XPath(
    ("descendant-or-self::a  [normalize-space(@href) and substring(normalize-space(@href),1,1) != '#'] |"
     "descendant-or-self::x:a[normalize-space(@href) and substring(normalize-space(@href),1,1) != '#']"),
    namespaces={'x':XHTML_NAMESPACE})

class HtmlCleaner(object):
    """
    Instances cleans the document of each of the possible offending
    elements.  The cleaning is controlled by attributes; you can
    override attributes in a subclass, or set them in the constructor.

    ``scripts``:
        Removes any ``<script>`` tags.

    ``javascript``:
        Removes any Javascript, like an ``onclick`` attribute. Also removes stylesheets
        as they could contain Javascript.

    ``comments``:
        Removes any comments.

    ``style``:
        Removes any style tags or attributes.

    ``links``:
        Removes any ``<link>`` tags

    ``meta``:
        Removes any ``<meta>`` tags

    ``page_structure``:
        Structural parts of a page: ``<head>``, ``<html>``, ``<title>``.

    ``processing_instructions``:
        Removes any processing instructions.

    ``embedded``:
        Removes any embedded objects (flash, iframes)

    ``frames``:
        Removes any frame-related tags

    ``forms``:
        Removes any form tags

    ``annoying_tags``:
        Tags that aren't *wrong*, but are annoying.  ``<blink>`` and ``<marquee>``

    ``remove_tags``:
        A list of tags to remove.  Only the tags will be removed,
        their content will get pulled up into the parent tag.

    ``kill_tags``:
        A list of tags to kill.  Killing also removes the tag's content,
        i.e. the whole subtree, not just the tag itself.

    ``allow_tags``:
        A list of tags to include (default include all).

    ``remove_unknown_tags``:
        Remove any tags that aren't standard parts of HTML.

    ``safe_attrs_only``:
        If true, only include 'safe' attributes (specifically the list
        from the feedparser HTML sanitisation web site).

    ``safe_attrs``:
        A set of attribute names to override the default list of attributes
        considered 'safe' (when safe_attrs_only=True).

    ``add_nofollow``:
        If true, then any <a> tags will have ``rel="nofollow"`` added to them.

    ``host_whitelist``:
        A list or set of hosts that you can use for embedded content
        (for content like ``<object>``, ``<link rel="stylesheet">``, etc).
        You can also implement/override the method
        ``allow_embedded_url(el, url)`` or ``allow_element(el)`` to
        implement more complex rules for what can be embedded.
        Anything that passes this test will be shown, regardless of
        the value of (for instance) ``embedded``.

        Note that this parameter might not work as intended if you do not
        make the links absolute before doing the cleaning.

        Note that you may also need to set ``whitelist_tags``.

    ``whitelist_tags``:
        A set of tags that can be included with ``host_whitelist``.
        The default is ``iframe`` and ``embed``; you may wish to
        include other tags like ``script``, or you may want to
        implement ``allow_embedded_url`` for more control.  Set to None to
        include all tags.

    This modifies the document *in place*.
    """
    cfg = threading.local()
    cfg.scripts = True
    cfg.javascript = True
    cfg.comments = True
    cfg.style = False
    cfg.links = True
    cfg.meta = True
    cfg.page_structure = True
    cfg.processing_instructions = True
    cfg.embedded = True
    cfg.frames = True
    cfg.forms = True
    cfg.annoying_tags = True
    cfg.remove_tags = None
    cfg.allow_tags = None
    cfg.kill_tags = None
    cfg.remove_unknown_tags = True
    cfg.safe_attrs_only = True
    cfg.safe_attrs = defs.safe_attrs
    cfg.add_nofollow = False
    cfg.host_whitelist = ()
    cfg.whitelist_tags = set(['iframe', 'embed'])

    def __init__(self, **kw):
        for name, value in kw.items():
            if not hasattr(self.cfg, name):
                raise TypeError(
                    "Unknown parameter: %s=%r" % (name, value))
            setattr(self.cfg, name, value)

    # Used to lookup the primary URL for a given tag that is up for
    # removal:
    _tag_link_attrs = dict(
        script='src',
        link='href',
        # From: http://java.sun.com/j2se/1.4.2/docs/guide/misc/applet.html
        # From what I can tell, both attributes can contain a link:
        applet=['code', 'object'],
        iframe='src',
        embed='src',
        layer='src',
        # FIXME: there doesn't really seem like a general way to figure out what
        # links an <object> tag uses; links often go in <param> tags with values
        # that we don't really know.  You'd have to have knowledge about specific
        # kinds of plugins (probably keyed off classid), and match against those.
        ##object=?,
        # FIXME: not looking at the action currently, because it is more complex
        # than than -- if you keep the form, you should keep the form controls.
        ##form='action',
        a='href',
        )

    def __call__(self, doc):
        """
        Cleans the document.
        """
        if hasattr(doc, 'getroot'):
            # ElementTree instance, instead of an element
            doc = doc.getroot()
        # convert XHTML to HTML
        xhtml_to_html(doc)
        # Normalize a case that IE treats <image> like <img>, and that
        # can confuse either this step or later steps.
        for el in doc.iter('image'):
            el.tag = 'img'
        if not self.cfg.comments:
            # Of course, if we were going to kill comments anyway, we don't
            # need to worry about this
            self.kill_conditional_comments(doc)

        kill_tags = set(self.cfg.kill_tags or ())
        remove_tags = set(self.cfg.remove_tags or ())
        allow_tags = set(self.cfg.allow_tags or ())

        if self.cfg.scripts:
            kill_tags.add('script')
        if self.cfg.safe_attrs_only:
            safe_attrs = set(self.cfg.safe_attrs)
            for el in doc.iter(etree.Element):
                attrib = el.attrib
                for aname in attrib.keys():
                    if aname not in safe_attrs:
                        del attrib[aname]
        if self.cfg.javascript:
            if not (self.cfg.safe_attrs_only and
                    self.cfg.safe_attrs == defs.safe_attrs):
                # safe_attrs handles events attributes itself
                for el in doc.iter(etree.Element):
                    attrib = el.attrib
                    for aname in attrib.keys():
                        if aname.startswith('on'):
                            del attrib[aname]
            doc.rewrite_links(self._remove_javascript_link,
                              resolve_base_href=False)
            if not self.cfg.style:
                # If we're deleting style then we don't have to remove JS links
                # from styles, otherwise...
                for el in _find_styled_elements(doc):
                    old = el.get('style')
                    new = _css_javascript_re.sub('', old)
                    new = _css_import_re.sub('', new)
                    if self._has_sneaky_javascript(new):
                        # Something tricky is going on...
                        del el.attrib['style']
                    elif new != old:
                        el.set('style', new)
                for el in list(doc.iter('style')):
                    if el.get('type', '').lower().strip() == 'text/javascript':
                        el.drop_tree()
                        continue
                    old = el.text or ''
                    new = _css_javascript_re.sub('', old)
                    # The imported CSS can do anything; we just can't allow:
                    new = _css_import_re.sub('', old)
                    if self._has_sneaky_javascript(new):
                        # Something tricky is going on...
                        el.text = '/* deleted */'
                    elif new != old:
                        el.text = new
        if self.cfg.comments or self.cfg.processing_instructions:
            # FIXME: why either?  I feel like there's some obscure reason
            # because you can put PIs in comments...?  But I've already
            # forgotten it
            kill_tags.add(etree.Comment)
        if self.cfg.processing_instructions:
            kill_tags.add(etree.ProcessingInstruction)
        if self.cfg.style:
            kill_tags.add('style')
            etree.strip_attributes(doc, 'style')
        if self.cfg.links:
            kill_tags.add('link')
        elif self.cfg.style or self.cfg.javascript:
            # We must get rid of included stylesheets if Javascript is not
            # allowed, as you can put Javascript in them
            for el in list(doc.iter('link')):
                if 'stylesheet' in el.get('rel', '').lower():
                    # Note this kills alternate stylesheets as well
                    if not self.allow_element(el):
                        el.drop_tree()
        if self.cfg.meta:
            kill_tags.add('meta')
        if self.cfg.page_structure:
            remove_tags.update(('head', 'html', 'title'))
        if self.cfg.embedded:
            # FIXME: is <layer> really embedded?
            # We should get rid of any <param> tags not inside <applet>;
            # These are not really valid anyway.
            for el in list(doc.iter('param')):
                found_parent = False
                parent = el.getparent()
                while parent is not None and parent.tag not in ('applet', 'object'):
                    parent = parent.getparent()
                if parent is None:
                    el.drop_tree()
            kill_tags.update(('applet',))
            # The alternate contents that are in an iframe are a good fallback:
            remove_tags.update(('iframe', 'embed', 'layer', 'object', 'param'))
        if self.cfg.frames:
            # FIXME: ideally we should look at the frame links, but
            # generally frames don't mix properly with an HTML
            # fragment anyway.
            kill_tags.update(defs.frame_tags)
        if self.cfg.forms:
            remove_tags.add('form')
            kill_tags.update(('button', 'input', 'select', 'textarea'))
        if self.cfg.annoying_tags:
            remove_tags.update(('blink', 'marquee'))

        _remove = []
        _kill = []
        for el in doc.iter():
            if el.tag in kill_tags:
                if self.allow_element(el):
                    continue
                _kill.append(el)
            elif el.tag in remove_tags:
                if self.allow_element(el):
                    continue
                _remove.append(el)

        if _remove and _remove[0] == doc:
            # We have to drop the parent-most tag, which we can't
            # do.  Instead we'll rewrite it:
            el = _remove.pop(0)
            el.tag = 'div'
            el.attrib.clear()
        elif _kill and _kill[0] == doc:
            # We have to drop the parent-most element, which we can't
            # do.  Instead we'll clear it:
            el = _kill.pop(0)
            if el.tag != 'html':
                el.tag = 'div'
            el.clear()

        _kill.reverse() # start with innermost tags
        for el in _kill:
            el.drop_tree()
        for el in _remove:
            el.drop_tag()

        if self.cfg.remove_unknown_tags:
            if allow_tags:
                raise ValueError(
                    "It does not make sense to pass in both allow_tags and remove_unknown_tags")
            allow_tags = set(defs.tags)
        if allow_tags:
            bad = []
            for el in doc.iter():
                if el.tag not in allow_tags:
                    bad.append(el)
            if bad:
                if bad[0] is doc:
                    el = bad.pop(0)
                    el.tag = 'div'
                    el.attrib.clear()
                for el in bad:
                    el.drop_tag()
        if self.cfg.add_nofollow:
            for el in _find_external_links(doc):
                if not self.allow_follow(el):
                    rel = el.get('rel')
                    if rel:
                        if ('nofollow' in rel
                                and ' nofollow ' in (' %s ' % rel)):
                            continue
                        rel = '%s nofollow' % rel
                    else:
                        rel = 'nofollow'
                    el.set('rel', rel)

    def allow_follow(self, anchor):
        """
        Override to suppress rel="nofollow" on some anchors.
        """
        return False

    def allow_element(self, el):
        if el.tag not in self._tag_link_attrs:
            return False
        attr = self._tag_link_attrs[el.tag]
        if isinstance(attr, (list, tuple)):
            for one_attr in attr:
                url = el.get(one_attr)
                if not url:
                    return False
                if not self.allow_embedded_url(el, url):
                    return False
            return True
        else:
            url = el.get(attr)
            if not url:
                return False
            return self.allow_embedded_url(el, url)

    def allow_embedded_url(self, el, url):
        if (self.cfg.whitelist_tags is not None
            and el.tag not in self.cfg.whitelist_tags):
            return False
        scheme, netloc, path, query, fragment = urlsplit(url)
        netloc = netloc.lower().split(':', 1)[0]
        if scheme not in ('http', 'https'):
            return False
        if netloc in self.cfg.host_whitelist:
            return True
        return False

    def kill_conditional_comments(self, doc):
        """
        IE conditional comments basically embed HTML that the parser
        doesn't normally see.  We can't allow anything like that, so
        we'll kill any comments that could be conditional.
        """
        bad = []
        self._kill_elements(
            doc, lambda el: _conditional_comment_re.search(el.text),
            etree.Comment)                

    def _kill_elements(self, doc, condition, iterate=None):
        bad = []
        for el in doc.iter(iterate):
            if condition(el):
                bad.append(el)
        for el in bad:
            el.drop_tree()

    def _remove_javascript_link(self, link):
        # links like "j a v a s c r i p t:" might be interpreted in IE
        new = _substitute_whitespace('', link)
        if _is_javascript_scheme(new):
            # FIXME: should this be None to delete?
            return ''
        return link

    _substitute_comments = re.compile(r'/\*.*?\*/', re.S).sub

    def _has_sneaky_javascript(self, style):
        """
        Depending on the browser, stuff like ``e x p r e s s i o n(...)``
        can get interpreted, or ``expre/* stuff */ssion(...)``.  This
        checks for attempt to do stuff like this.

        Typically the response will be to kill the entire style; if you
        have just a bit of Javascript in the style another rule will catch
        that and remove only the Javascript from the style; this catches
        more sneaky attempts.
        """
        style = self._substitute_comments('', style)
        style = style.replace('\\', '')
        style = _substitute_whitespace('', style)
        style = style.lower()
        if 'javascript:' in style:
            return True
        if 'expression(' in style:
            return True
        return False

    def clean_html(self, html):
        result_type = type(html)
        if isinstance(html, basestring):
            doc = fromstring(html)
        else:
            doc = copy.deepcopy(html)
        self(doc)
        return _transform_result(result_type, doc)

