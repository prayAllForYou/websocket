#!/usr/bin/python
#encoding:utf-8

from nose import with_setup
from util.log.log import log_handler

def testlog():
    log = log_handler()
    log.info("test info")
    log.debug("test debug")
    log.error("test error")
		 



