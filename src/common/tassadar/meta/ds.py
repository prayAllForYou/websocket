#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

from metabase import MetaBase
import common.tools as tools


class DS(MetaBase):
    """
    数据库数据源（用于数据同步）, 此表在本系统中不再使用，由DI负责维护管理
    """
    _table = 'DS'
    """字段定义"""
    DS_ID = 'ds_id'
    NAME = 'name'
    TYPE = 'type'
    DATABASE = 'database'
    SCHEMA = 'schema'
    CTIME = 'ctime'
    UTIME = 'utime'
    IS_DEL = 'is_del'
    OWNER = 'owner'
    PORT = 'port'
    USERNAME = 'username'
    PASSWORD = 'password'
    HOST = 'host'
    SYNC_CONFIG = 'sync_config'
    DB_ID = 'db_id'
    """数据源类型定义"""
    MYSQL_TYPE = 0
    ORACLE_TYPE = 1
    SQLSERVER_TYPE = 2

    def create(self, params):
        if DS.DS_ID not in params:
            params[DS.DS_ID] = tools.uniq_id('ds')

        if self._create(params):
            return params[DS.DS_ID]
        else:
            return None

    def get_one(self, ds_id, cols=(), filter_delete=True):
        res = self._get_one({DS.DS_ID: ds_id}, cols, filter_delete)
        if res and res.get(DS.SCHEMA):
            res[DS.SCHEMA] = json.loads(res[DS.SCHEMA])
        return res

    def get_list(self, owner, cols=(), offset=0, limit=1000):
        return self.get({DS.OWNER: owner}, cols, offset, limit)

    def update(self, ds_id, params, is_del=False):
        return self._update({DS.DS_ID: ds_id, 'is_del': is_del}, params)

    def delete(self, ds_id):
        return self._delete({DS.DS_ID: ds_id})
