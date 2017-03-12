#!/usr/bin/python
# -*- coding: utf-8 -*-
from common.redis_helper import RedisHelper
import json


class BaskTaskQueue:
    def __init__(self):
        self.task_queue_name = "task:common:queue"
        self.redis_helper = RedisHelper()

    def push(self, task_info):
        self.redis_helper.rpush(self.task_queue_name, json.dumps(task_info))

    def pop(self):
        task = self.redis_helper.lpop(self.task_queue_name)
        if task:
            return json.loads(task)
        return None


