#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging


class Logger(object):
    """
    日志辅助类
    """

    def __init__(self):
        super(Logger, self).__init__()
        logger_name = ('%s' % self.__class__).split('.')[-1]
        self.logger = logging.getLogger(logger_name)
