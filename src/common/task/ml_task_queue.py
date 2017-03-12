#!/usr/bin/python
# -*- coding: utf-8 -*-
from base_task_queue import BaskTaskQueue


class MLTaskQueue(BaskTaskQueue):

    def __init__(self):
        BaskTaskQueue.__init__(self)
        self.task_queue_name = "task:ml:queue"




