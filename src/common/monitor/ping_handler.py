#!/usr/bin/env python
# -*- coding: utf-8 -*-

from tornado.web import RequestHandler


class PingHandler(RequestHandler):

    def get(self, *args, **kwargs):

        self.set_status(200)
        self.write("Pong")

    def post(self, *args, **kwargs):

        self.set_status(200)
        self.write("Pong")
