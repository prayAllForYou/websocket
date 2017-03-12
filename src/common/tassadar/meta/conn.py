#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
"""
import common.config
from common.database import MySQLHelper

__author__ = 'stan'


def DBTassadar():
    conf = common.config.get_section('meta_database')
    return MySQLHelper("%s:%s" % (conf['host'], conf['port']), conf['user'], conf['passwd'], 'tassadar',
                       int(conf.get('pool_size', 10)), int(conf.get('max_overflow', 25)))
