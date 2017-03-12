#!/usr/bin/python
# encoding:utf-8

from redis_helper import RedisHelper
from json import dumps, loads
import common.config


class PubSubHelper(object):
    MSG_TYPE_TB_UPDATE = 1
    MSG_TYPE_FILE_EXPORT = 2

    def __init__(self):
        conf = common.config
        self.channel = "fe:ws:%s" % conf.get("global", "subpub_channel")
        self.handler = RedisHelper()

    def pub(self, msg):
        return self.handler.pub(self.channel, dumps(msg))

    def listen(self):
        c = self.handler.get_client(self.handler.MODE_READ)
        ps = c.pubsub()
        ps.subscribe(self.channel)
        for msg in ps.listen():
            if msg['type'] == "message":
                yield loads(msg['data'])
