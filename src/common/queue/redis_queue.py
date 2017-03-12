#!/usr/bin/python
# -*- coding: utf-8 -*-
from json import dumps, loads

import time
from redis.exceptions import RedisError

from common.redis_helper import RedisHelper
import common.config


class RedisQueue(object):
    def __init__(self):
        self.redis_key = "%s:%s" % (self._key_prefix(), self._key_suffix())
        self.handler = RedisHelper()

    def _key_prefix(self):
        return "tsd:fe"

    def _key_suffix(self):
        return "generic_key"

    def push(self, job):
        return self.handler.rpush(self.redis_key, dumps(job))

    def pop(self):
        topic = self.redis_key
        while True:
            try:
                # 必须对redis错误处理，否则由于这是一个生成器，DI进程将退出，
                is_empty = self.handler.llen(topic) == 0
            except RedisError:
                time.sleep(3)
                continue

            if is_empty:
                # 说明压力不是那么大，睡一会无妨，否则空转对redis压力较大
                time.sleep(1)
                continue

            try:
                res = self.handler.lpop(topic)
            except RedisError:
                time.sleep(3)
                continue

            if not res:
                # 说明被别人抢走了，还是压力不大，再睡会
                time.sleep(1)
                continue

            yield loads(res)

    def size(self):
        return self.handler.llen(self.redis_key)


class TableQueue(RedisQueue):
    def _key_suffix(self):
        return common.config.get("queue", "table_update_queue")


class ExportQueue(RedisQueue):
    def _key_suffix(self):
        return common.config.get("queue", "export_queue")


