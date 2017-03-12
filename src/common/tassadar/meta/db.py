#!/usr/bin/env python
# -*- coding: utf-8 -*-
from common.tassadar.meta.metabase import MetaBase
import common.tools as tools


class DB(MetaBase):
    """
    虚拟数据库
    """
    _table = 'DB'
    """字段定义"""
    DB_ID = 'db_id'
    OWNER = 'owner'
    NAME = 'name'
    TITLE = 'title'
    TYPE = 'type'
    STATUS = 'status'
    CTIME = 'ctime'
    UTIME = 'utime'
    """DB类型定义"""
    DS_TYPE = 0
    DB_TYPE = 1
    """数据库状态定义"""
    CREATE_STATUS = 0
    SYNC_FINISH = 1
    SYNC_ERROR = 2
    SYNCING_STATUS = 3

    def create(self, params):
        params[DB.DB_ID] = tools.uniq_id('db')
        params[DB.STATUS] = 0
        if self._create(params):
            return params[DB.DB_ID]
        else:
            return None

    def get_one(self, db_id, cols=(), filter_delete=True):
        res = self._get_one({DB.DB_ID: db_id}, cols, filter_delete)
        return res

    def get_list(self, owner, cols=(), offset=0, limit=1000):
        return self.get({DB.OWNER: owner}, cols, offset, limit)

    def update(self, db_id, params, is_del=False):
        return self._update({DB.DB_ID: db_id, 'is_del': is_del}, params)

    def delete(self, db_id):
        return self._delete({DB.DB_ID: db_id})
