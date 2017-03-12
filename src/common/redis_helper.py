#!/usr/bin/python
# -*- coding: utf-8 -*-


import json
import common.config
from redis.sentinel import Sentinel
from redis.client import Redis
import threading
import time
from logging import getLogger

import redis
if redis.__version__ != "2.10.5":
    raise Exception("redis must be 2.10.5 , others have some bug")

conf = common.config.get_section('redis')


def trace_time(func):

    def _with_time(*args, **kwargs):

        t1 = time.time()
        result = func(*args, **kwargs)
        t2 = time.time() - t1

        if t2 > 0.5:
            getLogger("performance").error("[REDIS] func [%s] cost [%f], args: %s", func.__name__, t2, args[1:])

        return result

    return _with_time


class RedisHelper(object):

    MODE_WRITE = 0
    MODE_READ = 1

    _lock = threading.Lock()

    redis_objs = {}

    def __init__(self):

        if self.__class__.redis_objs:
            return

        # 获得锁，采用非阻塞模式防死锁挂死
        locked = False
        retry = 0
        while not locked and retry < 10:
            locked = self.__class__._lock.acquire(False)
            time.sleep(0.01)
            retry += 1

        if not locked:
            raise Exception("programing error or system error in acquire lock")

        try:
            self.__class__.init_redis_client()
        finally:
            self.__class__._lock.release()

    @classmethod
    def init_redis_client(cls):

        # 防止多线程竞争，获取到锁后依然可能已经初始化了
        if cls.redis_objs:
            return

        cls.instance_name = conf.get('sentinel', "")
        cls.socket_timeout = conf.get("socket_timeout", 5)
        cls.connect_timeout = conf.get("connect_timeout", 0.1)

        if not cls.instance_name:
            # 单例模式
            host = conf["host"]
            cls.redis_objs[cls.MODE_READ] = Redis(
                host=host.split(':')[0],
                port=host.split(':')[1],
                socket_timeout=cls.socket_timeout,
                socket_connect_timeout=cls.connect_timeout,
                retry_on_timeout=1
            )

            cls.redis_objs[cls.MODE_WRITE] = cls.redis_objs[cls.MODE_READ]

        else:
            # 哨兵模式
            sentinel = Sentinel(
                cls.parse_config(),
                socket_timeout=cls.socket_timeout,
                socket_connect_timeout=cls.connect_timeout,
                retry_on_timeout=1
            )

            cls.redis_objs[cls.MODE_READ] = sentinel.slave_for(cls.instance_name)
            cls.redis_objs[cls.MODE_WRITE] = sentinel.master_for(cls.instance_name)

    @classmethod
    def parse_config(cls):
        cluster = conf['cluster']
        return [tuple(t.split(":")) for t in cluster.split(',')]

    def get_client(self, mode, timeout=None):

        # def _curr_session_time_out():
        #     return timeout if timeout is not None else self.sock_timeout

        if timeout is not None:

            # 如果指定了超时，就临时生成一个符合这个超时的连接，使用短连接，这个场景主要应对DI轮训服务，不能频繁超时

            if not self.__class__.instance_name:
                host = conf["host"]
                return Redis(
                    host=host.split(':')[0],
                    port=host.split(':')[1],
                    socket_timeout=self.socket_timeout+2,
                    socket_connect_timeout=self.connect_timeout,
                    retry_on_timeout=1,
                    socket_keepalive=False
                )

            curr_sentinel = Sentinel(
                self.parse_config(),
                socket_timeout=self.socket_timeout,
                socket_connect_timeout=self.connect_timeout,
            )

            if mode == self.MODE_WRITE:
                return curr_sentinel.master_for(
                    self.instance_name,
                    socket_timeout=timeout+2,
                    socket_connect_timeout=self.connect_timeout,
                    socket_keepalive=False,  # 短连接
                    retry_on_timeout=True
                )
            else:
                return curr_sentinel.slave_for(
                    self.instance_name,
                    socket_timeout=timeout+2,
                    socket_connect_timeout=self.connect_timeout,
                    socket_keepalive=False,
                    retry_on_timeout=True

                )
        else:
            return self.__class__.redis_objs[mode]

    # 普通key/value
    @trace_time
    def set(self, key, data, expire=None):
        c = self.get_client(self.MODE_WRITE)
        c.set(key, data)
        if expire is not None:
            c.expire(key, expire)
        return True

    # key/json
    @trace_time
    def set_json(self, key, data, expire=None):
        c = self.get_client(self.MODE_WRITE)
        c.set(key, json.dumps(data))
        if expire is not None:
            c.expire(key, expire)
        return True

    @trace_time
    def get(self, key, expire=None):
        """
        获取指定的key
        :param key:
        :param expire: 如果不为None，则按照指定的缓存时间为key设置缓存时间
        :return:
        """
        master = self.get_client(self.MODE_WRITE)
        slave = self.get_client(self.MODE_READ)
        data = slave.get(key)

        # 为了强一致性，如果从节点没有，主节点再读一下
        if not data:
            data = master.get(key)

        if expire and data:
            master.expire(key, expire)

        return data

    @trace_time
    def get_json(self, key, expire=None):
        data = self.get(key, expire)
        return {} if not data else json.loads(data)

    @trace_time
    def incr(self, key, num=1):
        c = self.get_client(self.MODE_WRITE)
        return c.incr(key, num)

    @trace_time
    def delete(self, key):
        c = self.get_client(self.MODE_WRITE)
        return c.delete(key)

    @trace_time
    def deleteN(self, key):
        master = self.get_client(self.MODE_WRITE)
        keys = self.keys("%s*" % key)
        for k in keys:
            master.delete(k)
        return True

    @trace_time
    def keys(self, key, cursor=0, count=100):
        res = []
        is_first = True
        slave = self.get_client(self.MODE_READ)
        while is_first or cursor > 0:
            is_first = False
            cursor, scan_keys = slave.scan(cursor, key, count)
            if scan_keys:
                res = res + scan_keys
        return list(set(res))

    @trace_time
    def scan(self, cursor, match, count):
        slave = self.get_client(self.MODE_READ)
        return slave.scan(cursor, match, count)

    @trace_time
    def exists(self, key):
        c = self.get_client(self.MODE_READ)
        return c.exists(key)

    @trace_time
    def hset(self, key, field, value):
        c = self.get_client(self.MODE_WRITE)
        return c.hset(key, field, value)

    @trace_time
    def hget(self, key, field):
        c = self.get_client(self.MODE_READ)
        return c.hget(key, field)

    @trace_time
    def hgetall(self, key):
        c = self.get_client(self.MODE_READ)
        return c.hgetall(key)

    @trace_time
    def hincrby(self, key, field, step):
        c = self.get_client(self.MODE_WRITE)
        return c.hincrby(key, field, step)

    @trace_time
    def hmset(self, name, mapping):
        c = self.get_client(self.MODE_WRITE)
        return c.hmset(name, mapping)

    @trace_time
    def eval(self, script, numkeys, *keys_and_args):
        c = self.get_client(self.MODE_WRITE)
        return c.eval(script, numkeys, *keys_and_args)

    @trace_time
    def hmget(self, key, fields):
        c = self.get_client(self.MODE_READ)
        return c.hmget(key, fields)

    @trace_time
    def flush(self, key):
        c = self.get_client(self.MODE_WRITE)
        keys = self.keys("%s*" % key)
        pipe = c.pipeline()
        for key in keys:
            pipe.delete(key)
        pipe.execute()
        return True

    @trace_time
    def set_ttl(self, key, expire=21600):
        c = self.get_client(self.MODE_WRITE)
        return c.expire(key, expire)

    @trace_time
    def pub(self, channel, msg):
        c = self.get_client(self.MODE_WRITE)
        c.pubsub()
        return c.publish(channel, msg)

    @trace_time
    def expire(self, key, expire):
        c = self.get_client(self.MODE_WRITE)
        return c.expire(key, expire)

    @trace_time
    def zrange(self, key, lowest, highest, withscores):
        c = self.get_client(self.MODE_READ)
        return c.zrange(key, lowest, highest, withscores)

    @trace_time
    def zscore(self, key, member):
        c = self.get_client(self.MODE_READ)
        return c.zscore(key, member)

    @trace_time
    def zadd(self, key, score, member):
        c = self.get_client(self.MODE_WRITE)
        return c.zadd(key, score, member)

    @trace_time
    def zrem(self, key, member):
        c = self.get_client(self.MODE_WRITE)
        return c.zrem(key, member)

    @trace_time
    def rpush(self, key, value):
        c = self.get_client(self.MODE_WRITE)
        return c.rpush(key, value)

    @trace_time
    def hdel(self, key, field):
        c = self.get_client(self.MODE_WRITE)
        return c.hdel(key, field)

    @trace_time
    def blpop(self, key, timeout):
        c = self.get_client(self.MODE_WRITE, timeout+2)
        return c.blpop(key, timeout)

    @trace_time
    def lpop(self, key):
        c = self.get_client(self.MODE_WRITE)
        return c.lpop(key)

    @trace_time
    def llen(self, key):
        c = self.get_client(self.MODE_READ)
        return c.llen(key)

    @trace_time
    def setnx(self, key, value):
        c = self.get_client(self.MODE_WRITE)
        return c.setnx(key, value)

    @trace_time
    def decr(self, key):
        c = self.get_client(self.MODE_WRITE)
        return c.decr(key)

    @trace_time
    def sadd(self, key, *values):
        c = self.get_client(self.MODE_WRITE)
        return c.sadd(key, *values)

    @trace_time
    def smembers(self, key):
        c = self.get_client(self.MODE_READ)
        return c.smembers(key)

if __name__ == '__main__':
    r = RedisHelper()
    if not r:
        print "连接失败"
    print "set test_key = test_value"
    r.set("test_key", "test_value")
    print "get test_key: ", r.get("test_key")

    print "set test_key = {'key': 'value'}"
    r.set_json("test_key", {'key': 'value'})
    print "get test_key: ", r.get_json("test_key")

    print "delete test_key"
    r.delete("test_key")
    print "hset hash test_key = {'key': 'value'}"

    r.hset("test_key", "key", "value")
    print "hget test_key-key: ", r.hget('test_key', 'key')
