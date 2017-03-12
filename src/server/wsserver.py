#!/usr/bin/python
#encoding:utf-8

import tornado.ioloop
import tornado.web
import tornado.httpserver
from tornado.options import define, options, parse_command_line
import router
import os
import common.config
from server.listen import sub_channel_Listen
import thread

port = int(common.config.get("global", "port"))
debug_mode = int(common.config.get("global", "server_debug_mode"))

settings = {
    "debug": debug_mode,
    "template_path": os.getenv("TEMPLATES"),
    "cookie_secret": "*1efff333d5548ec205fc2d6ea60708cb/sheldon!"
}
define("port", default=port, help="ws listen port")


def main():
    thread.start_new_thread(sub_channel_Listen, ())
    parse_command_line()
    log = "start ws api server at %s" % options.port
    print log
    app = tornado.web.Application(router.url_map, **settings)
    server = tornado.httpserver.HTTPServer(app, max_body_size=800*1024*1024)

    server.bind(options.port)
#     server.start(1 if debug_mode else 50)
    server.start(1)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()
