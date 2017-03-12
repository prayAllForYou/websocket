#!/usr/bin/python
#encoding:utf-8
from json import dumps
import traceback
import sockjs.tornado
from util.token.access_token_util import AccessTokenUtil
from util.user_pool.user_conn_pool import UserConnPool
from util.log.log import trace_error, trace_info

# user connection pool
user_conn_pool = UserConnPool()

class Connection(sockjs.tornado.SockJSConnection):
    """Chat connection implementation"""

    def on_open(self, info):
        try:
            self.user_id = ""
            self._init(info)
            if self._token_check():
                # Add client to the clients list
                self.user_id = self.atHelper.user_id()
                user_conn_pool.add(self.user_id, self)
                self.send_msg(0, {"system": "The connection is successful"})
                trace_info("add conn => %s" % self.user_id )
            else:
                self.send_msg(1, {"error": "token error"})
                self.close()
        except Exception,e:
            exception_msg = traceback.format_exc()
            print exception_msg
            trace_error("Unhandled exception caught in req exception_msg => %s" % exception_msg)
        
    def on_message(self, message):
        # Broadcast message
        self.broadcast(user_conn_pool.get(self.user_id), message)

    def on_close(self):
        try:
            # Remove client from the clients list and broadcast leave message
            if self.user_id:
                user_conn_pool.remove(self.user_id, self)
                trace_info("remove conn => %s" % self.user_id )
        except Exception,e:
            exception_msg = traceback.format_exc()
            print exception_msg
            trace_error("Unhandled exception caught in req exception_msg => %s" % exception_msg)
        
    def _token_check(self):
        try:
            return self.atHelper.verify_token()
        except Exception,e:
            exception_msg = traceback.format_exc()
            trace_error("Unhandled exception caught in req exception_msg => %s" % exception_msg)
            return False
    
    def _init(self, info):
        cookie_token = info.get_cookie('token').value if info.get_cookie('token') else None
        param_token = info.get_argument("token")
        token = param_token if param_token else cookie_token
        ip = info.ip
        self.atHelper = AccessTokenUtil(token, ip)
    
    # state 0:正常连接  1：已关闭连接
    def send_msg(self, state, msg):
        msg.update({"state": state})
        self.send(dumps(msg))
        
    
    
    