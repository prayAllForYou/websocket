#!/usr/bin/env python
# -*- coding:utf-8 -*-


class DiException(Exception):
    def __init__(self, status, errstr, uri=""):
        self.status = status
        self.errstr = errstr
        self.uri = uri

    def __str__(self):
        return self.errstr
