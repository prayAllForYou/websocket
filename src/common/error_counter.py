#!/usr/bin/python
# encoding:utf-8


import common.config
from datetime import *
from redis_helper import RedisHelper


class ErrorCounter():
    def __init__(self):
        self.key_prefix = "err_cnt:%s" % common.config.get("global", "error_counter_prefix")
        self.handler = RedisHelper()

    def incr(self):
        dt = datetime.now()
        key = "%s:%s" % (self.key_prefix, dt.strftime("%Y%m%d"))
        field = dt.strftime("%H%M")
        res = self.handler.hincrby(key, field, 1)
        self.handler.expire(key, 86400)
        return res
