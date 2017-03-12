#!/usr/bin/env python
# -*- coding: utf-8 -*-

from metabase import MetaBase


class FieldSelected(MetaBase):
    """
    字段勾选状态
    """
    _table = 'FIELD_SELECTED'

    """字段定义"""
    TB_ID = 'tb_id'
    FIELD_IDS = 'field_ids'
    CTIME = 'ctime'
    UTIME = 'utime'
    IS_DEL = 'is_del'

    def create(self, tb_id, field_ids):
        return self._create({
            FieldSelected.TB_ID: tb_id,
            FieldSelected.FIELD_IDS: field_ids
        })

    def update(self, tb_id, params):
        return self._update({FieldSelected.TB_ID: tb_id}, params)

    def get_one(self, tb_id, cols=()):
        res = self._get_one({FieldSelected.TB_ID: tb_id}, cols)
        return res
