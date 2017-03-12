#!/usr/bin/python
# encoding:utf-8
from handler.base.base_handler import BaseHandler
from handler.sock_server.conn_handler import user_conn_pool
from common.queue.redis_queue import TableQueue


class ClientListHandler(BaseHandler):
    
    def do_action(self):
        user_len_dict = user_conn_pool.user_conn_overview()
        table_queue = TableQueue()
        size = table_queue.size()
        self.result = {
            "user_len_dict": user_len_dict,
            "table_queue_size": size
            }
        return True