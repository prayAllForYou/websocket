#!/usr/bin/env python
# -*- coding: utf-8 -*-

from logging import getLogger
import os
from urlparse import urlparse
from requests.sessions import Session
from requests.adapters import ReadTimeout
import random
import time
from requests.exceptions import ConnectionError, HTTPError
from common import config
from threading import local


class ConnectException(Exception):
    pass


class BaseClient(object):

    _thread_vars = local()

    MODULE_NAME = ""

    PERFORMANCE = "performance"

    _global_sessions = {}

    @classmethod
    def set_trace_id(cls, trace_id):
        cls._thread_vars.trace_id = trace_id

    def __init__(self):
        self.url_prefix = None
        self.timeout = None

        self.init_config()

    @classmethod
    def debug_out(cls, *args, **kwargs):
        getLogger(cls.PERFORMANCE).debug(*args, **kwargs)

    @classmethod
    def trace_out(cls, *args, **kwargs):
        getLogger(cls.PERFORMANCE).info(*args, **kwargs)

    @classmethod
    def warn_out(cls, *args, **kwargs):
        getLogger(cls.PERFORMANCE).warning(*args, **kwargs)

    @classmethod
    def error_out(cls, *args, **kwargs):
        getLogger(cls.PERFORMANCE).error(*args, **kwargs)

    def init_config(self):

        self.url_prefix = [val.strip() for val in config.get(self.CONF_SECTION, self.KEY_NAME).split(',')]
        cs = config.get_section(self.CONF_SECTION)

        if "timeout" in cs:
            self.timeout = float(cs["timeout"])

    def pre_params(self):
        return {}

    @classmethod
    def get_session(cls, url):

        """
        根据要访问的url找到合适的session，尽可能重用连接
        """

        urlkry = "://".join(urlparse(url)[0:2])

        if urlkry not in cls._global_sessions:
            cls._global_sessions[urlkry] = Session()

        return cls._global_sessions[urlkry]

    def _do_request(self, short_uri, payload, files=None):
        payload.update(self.pre_params())

        payload["trace_id"] = getattr(self.__class__._thread_vars, "trace_id", "")

        res = None
        self.url_prefix.sort(key=lambda f: random.randint(0, 100))
        for curr_url_prefix in self.url_prefix:
            url = os.path.join(curr_url_prefix, short_uri)
            try:
                ts = time.time()
                c_session = self.get_session(url)
                self.debug_out("requesting [%s]... [%s]", url, payload["trace_id"])

                params_dict = dict(data=payload, timeout=self.timeout)
                if files is not None:
                    params_dict.update(files=files)

                res = c_session.post(url, **params_dict)
                res.raise_for_status()
                tc = time.time() - ts

                if tc >= 0.8:
                    self.error_out("[%s] [%f] [%s]", url, tc, payload["trace_id"])
                elif tc >= 0.3:
                    self.warn_out("[%s] [%f] [%s]", url, tc, payload["trace_id"])
                else:
                    self.trace_out("[%s] [%f] [%s]", url, tc, payload["trace_id"])

                break
            except ConnectionError:
                # try_count += 1
                # time.sleep(1)
                self.warn_out("connect failed to %s [%s]", curr_url_prefix, payload["trace_id"])
            except ReadTimeout:
                self.error_out("Timeout for url: %s [%s]", url, payload["trace_id"])
                raise
            except HTTPError:
                self.error_out("bad response: %s [%s]", res.text, payload["trace_id"])

        if res is None:
            raise ConnectException("connect upstream error. all backend tried %s" % self.url_prefix)

        return res

