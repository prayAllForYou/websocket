#!/usr/bin/python
# encoding:utf-8
import time
import json, traceback
from common.pub_sub_helper import PubSubHelper
from handler.sock_server.conn_handler import user_conn_pool
from util.log.log import trace_error,trace_info

def sub_channel_Listen():
    while True:
        try:
            trace_info("start sub channel")
            pub_subHelper = PubSubHelper()
            for msg in pub_subHelper.listen():
                trace_info("receive msg => %s" % json.dumps(msg))
                try:
                    user_id = msg['user_id']
                    for conn in user_conn_pool.get(user_id):
                        if conn.is_closed:
                            user_conn_pool.remove(user_id, conn)
                            trace_info("except remove conn => %s" % user_id )
                        else:
                            conn.send_msg(0, msg)
                            trace_info("send msg => %s" % json.dumps(msg) )
                except:
                    # write into trace_error log
                    exception_msg = traceback.format_exc()
                    print "exception_msg===>", exception_msg
                    trace_error("Unhandled exception caught in req exception_msg => %s, msg => %s" % ( exception_msg, json.dumps(msg)))
        except:
            # write into trace_error log
            exception_msg = traceback.format_exc()
            print "exception_msg===>", exception_msg
            trace_error("Unhandled exception caught in req exception_msg => %s" % exception_msg)
        trace_info("stop sub channel")
        time.sleep(1)
        