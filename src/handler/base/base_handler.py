#!/usr/bin/python
# encoding:utf-8

import sys
import uuid
import tornado.locale
import tornado.web
import tornado.ioloop
import functools
import time
import json
import traceback
import ERROR
from tornado.web import RequestHandler
from tornado.gen import coroutine
from tornado.gen import Task
from random import randint
from util.token.access_token_util import AccessTokenUtil
from util.http.request_context import HttpRequestContext
from urllib import quote
from common.overlord.client import OverlordException
from util.formater.str_format import ignore_carriage_return
from util.log.log import log_handler
from util.log.log import trace_error

reload(sys)
sys.setdefaultencoding('utf-8')


class BaseHandler(RequestHandler):

    error_code = ERROR
    log = log_handler()
    data_type = 'json'
    filename = ''
    no_valid_token_path = (
        r"/api/client/index",
        r"/api/client/list"
    )

    def __init__(self, application, request, **kwargs):
        RequestHandler.__init__(self, application, request, **kwargs)

    @coroutine
    def head(self):
        yield Task(self.run)
        self.do_response()

    @coroutine
    def get(self):
        yield Task(self.run)
        self.do_response()

    @coroutine
    def post(self):
        yield Task(self.run)
        self.do_response()

    def do_response(self):
        if self.data_type == 'json':
            data = {'status': '%s' % self.status, 'errstr': self.errstr, 'trcid': str(self.trace_id),
                    'result': self.result}
            jdata = json.dumps(data)
            self.add_header("Content-Type", "application/json")
            if self.call_back:
                self.add_header("Access-Control-Allow-Origin", "*")
                jdata = "%s(%s)" % (self.call_back, jdata)
            self.add_header("Content-Type", "application/json;charset=utf-8")
            self.write(jdata)
        elif self.data_type == 'png':
            self.add_header("Access-Control-Allow-Origin", "*")
            self.add_header("Content-Type", "image/png;charset=utf-8")
            self.add_header("Content-Disposition", "attachment; filename=\"%s.png\"; filename*=utf8\'\'%s.png" % (quote(self.filename.encode('utf-8')) if self.filename else "chart", quote(self.filename.encode('utf-8')) if self.filename else "chart"))
            self.write(self.result)
        elif self.data_type == 'gif':
            self.add_header("Content-Type", "image/gif;charset=utf-8")
            self.write(self.result)
        elif self.data_type == 'excel':
            self.add_header("Access-Control-Allow-Origin", "*")
            self.add_header("Content-Type", "application/vnd.ms-excel;charset=utf-8")
            self.add_header("Content-Disposition", "attachment; filename=\"%s.xlsx\"; filename*=utf8\'\'%s.xlsx" % (quote(self.filename.encode('utf-8')) if self.filename else "chart", quote(self.filename.encode('utf-8')) if self.filename else "chart"))
            self.write(self.result)
        elif self.data_type == 'file':
            self.add_header("Access-Control-Allow-Origin", "*")
            self.add_header("Content-Type", "application/octet-stream;charset=utf-8")
            self.add_header("Content-Disposition", "attachment; filename=\"%s\"; filename*=utf8\'\'%s" % (quote(self.filename.encode('utf-8')) if self.filename else "filename", quote(self.filename.encode('utf-8')) if self.filename else "filename"))
            self.write(self.result)
        else:
            self.write(self.result)

    @coroutine
    def _execute(self, transforms, *args, **kwargs):
        # 初始化一个http请求context，这个字典会贯彻整个http请求，包括不继承base_handler的util类
        # 下游的类只需调用HttpRequestContext.data就可以 get/set 这个字典
        self.trace_id = uuid.uuid3(uuid.NAMESPACE_DNS,
                                   "%s_%s_%s" % (self.request.path, time.time(), randint(0, 100000)))
        http_context = {
            "trace_id": self.trace_id,
            "request": self.request,
            "ip": self.get_ip(),
            "ua": self.get_ua(),
        }
        # TODO http_context的初始化 现阶段只用来做全局logging，纪录一个全局trace_id
        with tornado.stack_context.StackContext(functools.partial(HttpRequestContext, **http_context)):
            super(BaseHandler, self)._execute(transforms, *args, **kwargs)

    @coroutine
    def run(self, *args, **kwargs):
        '''返回response'''
        try:
            if self.validate():
                self.token_init()
                self.do_action()
        except Exception, e:
            if type(e) is OverlordException:
                trace_error(
                    "Unhandled exception %s, status: %s, errstr: %s" % (type(e), e.status, e.errstr))
                self.set_error(e.status, e.errstr)
            else:
                self.set_error(self.error_code.API_INTERNAL_ERROR, "Internal Server Error")
                sys.stderr.write(repr(traceback.print_exc()))
                sys.stderr.flush()
                # write into trace_error log
                exception_msg = traceback.format_exc()
                trace_error("Unhandled exception caught in req %s => %s" % (self.trace_id, exception_msg))
                
    def validate(self):
        # 验证cookie是否过期
        if not self.valid_token():
            self.set_error(self.error_code.COOKIE_EXPIRED, 'cookie 过期')
            return False
        return True

    def valid_token(self):
        '''排除不需要验证token的uri'''
        path = self.request.path
        if (path in self.no_valid_token_path):
            return True
        else:
            return self.atHelper.verify_token()
        
    def token_init(self):
        self.user_id = self.atHelper.user_id()
        self.role = self.atHelper.role()
        self.domain = self.atHelper.domain()
        self.user_info = self.atHelper.user_info()
        self.is_admin = self.atHelper.is_admin()
        self.enterprise_type = self.atHelper.enterprise_type()

    def prepare(self):
        self.request_start_time = time.time()
        self.token = self.get_argument('token', '')
        self.ua = self.get_ua()
        self.atHelper = AccessTokenUtil(self.token, self.get_ip(), self.ua)
        self.call_back = self.get_argument('callback', '')
        self.ip = self.get_ip()
        self.role = ''
        self.status = 0
        self.errstr = ''
        self.result = ''

    def on_finish(self):
        request_end_time = time.time()
        self.cost_time = int((request_end_time - self.request_start_time)*1000)

        # 设置log的参数
        if self.status == 0:
            self.log.info(self.get_log_info())
        else:
            self.log.error(self.get_log_info())
            
    def get_log_info(self):
        try:
            uri = self.request.path
            ip = self.ip
            # 如果uri里面包含password字段，则在日志里隐藏真实密码，用password_exists代替
            # 限制下单行日志的总长度
            request_log = '|'.join(self._get_request_parameters())
            log = "[%s]\t[%s]\t[%s]\t[%s]" % (ip, uri, self.user_id, request_log)
            log += "\t[ts=%s]\t[%s]\t[%s]\t[%s]" % (
                self.cost_time, self.status, self.errstr, self.trace_id
            )
            return log
        except:
            traceback.print_exc()
            
    def _get_request_parameters(self):
        argvs = []
        for i in self.request.arguments.items():
            if i[0].lower() != "password":
                argv = ignore_carriage_return(self.get_argument(i[0])[0:4096])
            else:
                argv = "password_exists"
            argvs.append("%s=%s" % (i[0], argv))
        return argvs

    def set_error(self, err_code, err_str, result=''):
        self.status = '%s' % err_code
        self.errstr = err_str
        print "status code = %s, errstr = %s" % (self.status, self.errstr)
        self.result = result

    def do_action(self):
        """handler 重写"""
        return True

    def get_ip(self):
        real_ip = self.request.headers.get('X-Forwarded-For', self.request.headers.get('X-Real-Ip', self.request.remote_ip))
        ip = real_ip.split(',')[0].strip()
        return ip

    def get_ua(self):
        user_agent = self.request.headers.get('User-Agent', "NotFound")
        return user_agent
