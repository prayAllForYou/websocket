#!/usr/bin/python
# encoding:utf-8

import sys
import time
import uuid

reload(sys)
sys.setdefaultencoding('utf-8')


def singleton(cls, *args, **kw):
    instances = {}

    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]

    return _singleton


def enum(**enums):
    return type('Enum', (), enums)


def current_time(string=True):
    now = int(time.time())
    if string:
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now))
    return now


def uniq_id(prefix=None):
    i = str(uuid.uuid4()).replace('-', '')
    if not prefix:
        return i
    else:
        return '%s_%s' % (prefix, i)


def escape(value):
    if not value:
        return value
    if isinstance(value, (tuple, list, dict)):
        return value
    return str(value).replace("'", "\\'").replace('"', '\\"')


class ObjParams(dict):
    def __getattr__(self, attr):
        try:
            return self[attr]
        except:
            return None

    def __setattr__(self, key, value):
        self[key] = value

if __name__ == '__main__':
    print uniq_id()
