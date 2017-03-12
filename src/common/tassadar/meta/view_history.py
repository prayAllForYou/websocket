#!/usr/bin/python
# encoding:utf8
from metabase import MetaBase


class ViewHistory(MetaBase):
    """
    虚拟数据表
    """
    _table = 'VIEW_HISTORY'
    '''字段类型定义'''
    TB_ID = 'tb_id'
    STORAGE_ID = 'storage_id'
    CTIME = 'ctime'

    def create(self, tb_id, storage_id):
        return self._create({ViewHistory.TB_ID: tb_id, ViewHistory.STORAGE_ID: storage_id})
