#!/usr/bin/python
#encoding:utf-8
from handler.base.base_handler import BaseHandler

class IndexHandler(BaseHandler):
    """Regular HTTP handler to serve the chatroom page"""
    def do_action(self):
        self.data_type = ""
        self.render('index.html')