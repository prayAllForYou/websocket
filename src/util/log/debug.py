#!/usr/bin/python
#encoding:utf-8

import sys
from random import randint

reload(sys)
sys.setdefaultencoding("utf-8")

class LogColor:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'

LogColors = ['\033[95m', '\033[94m', '\033[92m', '\033[93m', '\033[91m']
SIZE = len(LogColors)

class Debug:

    def __init__(self):
        self.out_writer = sys.stderr.write
    
    def debug(self, TAG, message):
        output = "%sTAG=>%s\t%s\t%s\n" % (LogColors[randint(0, SIZE-1)], TAG, repr(message), LogColor.ENDC)
        self.out_writer(output)

dbg = Debug()
def dodebug(tag, message):
    dbg.debug(tag, message)
