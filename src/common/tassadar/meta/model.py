#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from metabase import MetaBase
import common.tools as tools
from tb import TB



class Model(MetaBase):
    """
    模型表
    """
    _table = 'MODEL'
    '''字段类型定义'''
    MDL_ID = 'mdl_id'
    TB_ID = 'tb_id'
    USER_ID = 'user_id'
    NAME = 'name'
    TYPE = 'type'
    STATUS = 'status'
    CTIME = 'ctime'
    UTIME = 'utime'
    META = 'meta'
    TRAIN_RESULT = 'train_result'
    VERSION = 'version'

    '''模型类型定义'''
    LINEAR_REGRESSION = 1

    '''模型状态定义'''
    FINISH = 0
    QUEUE_UP = 1
    TRAINING = 2
    TRAIN_ERROR = 3

    def create(self, params):
        # 检查必填字段
        required_params = (Model.TB_ID, Model.USER_ID, Model.META, Model.NAME)
        for rp in required_params:
            if rp not in params:
                self.logger.warn('缺失字段：%s' % rp)
                return None

        if Model.MDL_ID not in params:
            params[Model.MDL_ID] = tools.uniq_id('mdl')
        params['version'] = 1
        # 表字段处理

        if self._create(params):
            return params[Model.MDL_ID]
        else:
            return None

    # 加入user_id根据当前user_id和groups，附加其特有的计算字段
    def get_model(self, mdl_id):
        """
        获取模型的基本信息
        :param :
        :param cols:mdl_id
        :return:
        """
        fields = [Model.MDL_ID, Model.TB_ID, Model.USER_ID, Model.NAME, Model.META, Model.TRAIN_RESULT, Model.STATUS, Model.UTIME, Model.VERSION, Model.TYPE]
        model_info = self._get_one({Model.MDL_ID: mdl_id}, fields, True)
        if model_info:
            model_info[Model.META] = json.loads(model_info[Model.META])
            model_info[Model.TRAIN_RESULT] = json.loads(model_info[Model.TRAIN_RESULT])
            tb_info = TB().get_tb_info(model_info['tb_id'], model_info['user_id'], filter_delete=False, cols=('title',))
            if tb_info:
                model_info['tb_name'] = tb_info['title']
            return model_info
        else:
            return {}

    def get_one(self, conds, cols=(), filter_delete=True):
        res = self._get_one(conds, cols, filter_delete)
        if not res:
            return None
        return res

    def get_list(self, user_id, cols=(), offset=0, limit=1000):
        return self.get({Model.USER_ID: user_id}, cols, offset, limit)

    def update(self, mdl_id, params):
        return self._update({Model.MDL_ID: mdl_id, 'is_del': 0}, params)

    def delete(self, mdl_id):
        return self._delete({Model.MDL_ID: mdl_id})

