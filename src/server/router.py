#!/usr/bin/python
# encoding:utf-8
import sockjs.tornado
from handler.client.index import IndexHandler
from handler.sock_server.conn_handler import Connection
from handler.client.list_handler import ClientListHandler

# 1. Create chat router
ChatRouter = sockjs.tornado.SockJSRouter(Connection, '/api/chat')

url_map = [
    (r"/api/client/index", IndexHandler),
    (r"/api/client/list", ClientListHandler),
    
] + ChatRouter.urls
