# -*- coding: utf-8 -*-

"""Provides utilities for retrieving website content."""


from furl import furl
from numpy.random import choice as random_choice
from random_user_agent.params import SoftwareName as SN, OperatingSystem as OS
from random_user_agent.user_agent import UserAgent
import requests_html
from selectolax.parser import HTMLParser
from selenium import webdriver
import pandas as pd
from time import strftime
from utilities import Logger
from utilities.decorators import error_trap


def parse_text(html):
    tree = HTMLParser(html)

    if tree.body is None:
        return None

    for tag in tree.css('script'):
        tag.decompose()
    for tag in tree.css('style'):
        tag.decompose()

    return tree


class Crawler(object):
    """Provides utilities for retrieving website content."""

    def __init__(self, **kwargs):
        agent_type = kwargs.setdefault('agent_type', None)
        referer = kwargs.setdefault('referer', None)
        self.headers = kwargs.setdefault('headers', HeaderGenerator().
                                         header(agent_type=agent_type,
                                                referer=referer))
        self.timeout = kwargs.setdefault('timeout', 10)
        self.resp_attributes = kwargs.setdefault('resp_attributes',
                                                 ['content', 'encoding',
                                                  'headers', 'history', 'html',
                                                  'json', 'ok', 'reason',
                                                  'status_code', 'text', 'url'])
        self.elem_attributes = kwargs.setdefault('elem_attributes',
                                                 ['absolute_links',
                                                  'base_url', 'encoding',
                                                  'full_text', 'html',
                                                  'links', 'raw_html', 'text',
                                                  'url'])
        self._logging = kwargs.setdefault('logging', True)
        if self._logging:
            log_path = kwargs.setdefault('log_path', '.')
            log_file = kwargs.setdefault('log_file', 'out.log')
            log_name = kwargs.setdefault('log_name', __name__)
            self._logger = Logger(name=log_name, log_path=log_path,
                                  log_file=log_file)
        self.session = requests_html.HTMLSession()
        self._err_recs = []

    def _push_error(self, error, url, comp_id=None, attr=None):
        c_id = str(comp_id) if comp_id else comp_id
        if self._logging:
            if c_id:
                msg = ('\nRequest for response from {} for company {} ' +
                       'threw exception: {}\n').format(url, c_id, error)
            elif attr:
                msg = ('\nRequest for "{}" from {} threw exception: {}\n'.
                       format(attr, url, error))
            else:
                msg = ('\nRequest for response from {} threw exception: {}\n'.
                       format(url, error))
            self._logger.error(msg)
        self._err_recs.append({'time': strftime('%Y-%m-%d %H:%M:%S'),
                               'company_profile_id': c_id,
                               'attribute': attr, 'url': url,
                               'exception': error})

    @error_trap
    def _get_response(self, url, headers, timeout, cookies):

        r = self.session.get(url, headers=headers,
                             timeout=timeout, cookies=cookies)
        if r is None:
            return None, None, url
        else:
            if r.ok:
                if self._logging:
                    self._logger.info(('\nOrig_URL: {}; Ret_URL: {}; ' +
                                       'status: {}, reason: {}\n').
                                      format(url, r.url, r.status_code,
                                             r.reason))
                return r, None, r.url
            else:
                if self._logging:
                    self._logger.warning(('\nOrig_URL: {}; Ret_URL: {}; ' +
                                          'status: {}, reason: {}\n').
                                         format(url, r.url, r.status_code,
                                                r.reason))
                return r, r.reason, r.url

    def response(self, url, headers=None, timeout=None,
                 cookies=None, c_id=None):
        headers = headers or self.headers
        timeout = timeout or self.timeout

        def flip_scheme():
            u = furl(url)
            u.scheme = 'https' if u.scheme == 'http' else 'http'
            return u.url

        f_val, err = self._get_response(url, headers, timeout, cookies)
        if err or f_val[1] or f_val[0] is None:
            flipped_url = flip_scheme()
            f_val, err = self._get_response(flipped_url, headers, timeout,
                                            cookies)
            if err:
                self._push_error(err, flipped_url, comp_id=c_id)
                return None
            else:
                if f_val[0] is None:
                    self._push_error('Response is NULL', flipped_url,
                                     comp_id=c_id)
                if f_val[1]:
                    self._push_error(f_val[1], flipped_url, comp_id=c_id)
                return f_val[0]
        else:
            return f_val[0]

    @error_trap
    def _check_valid_get(self, obj, a):
        obj_type = type(obj)
        if obj_type == requests_html.HTMLResponse:
            assert a in self.resp_attributes, \
                ('Second parameter must be one of: {}'.
                 format(', '.join(self.resp_attributes)))
        elif ((obj_type == requests_html.HTML) or
              (obj_type == requests_html.Element)):
            assert a in self.elem_attributes, \
                ('Second parameter must be one of: {}'.
                 format(', '.join(self.elem_attributes)))
        else:
            raise TypeError('First parameter must be one of type ' +
                            'requests_html.HTMLResponse, ' +
                            'requests_html.HTML, or ' +
                            'requests_html.Element')
        return

    @error_trap
    def _get(self, obj, a):
        _, err = self._check_valid_get(obj, a)
        if err:
            if type(err) == AssertionError:
                u = self.get(obj, 'url')
            else:
                u = None
            self._push_error(err, u, attr=a)
            return None
        else:
            attr = getattr(obj, a) if a != 'json' else getattr(obj, a)()
            if attr is None:
                u = self.get(obj, 'url') if a != 'url' else None
                self._push_error('NULL attribute', u, attr=a)
            return attr

    def get(self, obj, a):
        attr, err = self._get(obj, a)
        if err:
            u, e = self._get(obj, 'url') if a != 'url' else None, None
            if e:
                self._push_error(e, u, attr='url')
            self._push_error(err, u, attr=a)
        return attr

    @error_trap
    def _write_errors(self, outfile):
        ft = outfile.split('.')[-1]
        assert (ft in ['pkl', 'xlsx', 'csv']), \
            'Output filename must specify a pickle (.pkl), ' + \
            'excel (.xlsx) or csv (.csv) file.'
        if ft == 'pkl':
            pd.DataFrame(self._err_recs).to_pickle(outfile)
        elif ft == 'xlsx':
            pd.DataFrame(self._err_recs).to_excel(
                outfile, engine='xlsxwriter', index=False)
        else:
            pd.DataFrame(self._err_recs).to_csv(outfile, index=False)
        return outfile

    def write_errors(self, out_fn):
        outfile, err = self._write_errors(out_fn)
        if err:
            if self._logging:
                msg = '\nError while writing out error log: {}\n'.format(err)
                self._logger.error(msg)
        return outfile


class HeaderGenerator(UserAgent):
    def __init__(self, software_names=[SN.CHROME.value, SN.FIREFOX.value],
                 op_systems=[OS.LINUX.value, OS.WINDOWS.value, OS.MACOS.value],
                 referers=['https://www.google.com', 'https://duckduckgo.com',
                           'https://www.bing.com', 'https://www.yahoo.com'],
                 agent=None, agent_type=None):

        self._referers = referers
        super().__init__(software_names=software_names,
                         operating_systems=op_systems, limit=100)
        self.agent = agent

    def get_agent(self, agent_type=None):
        if agent_type == 'auto':
            return self.get_random_user_agent()
        elif agent_type == 'googlebot':
            return ('Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; ' +
                    'compatible; Googlebot/2.1; ' +
                    '+http://www.google.com/bot.html) Safari/537.36')
        else:
            return self.agent or self.get_random_user_agent()

    def get_referer(self):
        return random_choice(self._referers)

    def header(self, agent_type='auto', referer=None):
        h = {'user_agent': self.get_agent(agent_type)}
        if referer == 'auto':
            h['referer'] = self.get_referer()
        elif referer:
            h['referer'] = referer
        return h


class WebDriver(webdriver.Chrome):
    def __init__(headless=True, window_size='1200x600'):
        options = webdriver.ChromeOptions()
        options.add_argument('window-size=' + window_size)
        if headless:
            options.add_argument('headless')
        super().__init__(options=options)
