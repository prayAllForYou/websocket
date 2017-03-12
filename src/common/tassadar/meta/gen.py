#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

import common.tools as tools
from metabase import MetaBase


class Gen(MetaBase):
    """
    View的生成规则信息
    """
    _table = 'GEN'
    """字段定义"""
    OWNER = 'owner'
    GEN_ID = 'gen_id'
    TB_ID = 'tb_id'
    PARAM = 'param'
    TYPE = 'type'
    RESULT = 'result'
    ENGINE = 'engine'
    CTIME = 'ctime'
    UTIME = 'utime'
    DEPENDENT = 'dependent'
    USAGE = 'usage'
    MATERIALIZATION = 'materialization'
    VALIDATION_RESULT = 'validation_result'
    # 合表类型
    JOIN_TYPE = 0
    AGGR_TYPE = 1
    UNION_TYPE = 2
    SCRIPT_TYPE = 3
    EXTRACT_TYPE = 4
    ETL_TYPE = 5
    SHADOW_TYPE = 6

    def get_view_info(self, tb_id):
        """
        获取创建view的初始信息
        :param tb_id:
        :return:
        """
        return self._get_one({Gen.TB_ID: tb_id}, (
            Gen.GEN_ID, Gen.TYPE, Gen.PARAM, Gen.RESULT, Gen.MATERIALIZATION, Gen.DEPENDENT, Gen.VALIDATION_RESULT, Gen.OWNER))

    def create(self, owner, tb_id, type, param, result, dependent='', materialization=0):
        gen_id = tools.uniq_id('gen')
        usage = {"chart": {}, "total": 0}
        if self._create({
            Gen.OWNER: owner,
            Gen.TB_ID: tb_id,
            Gen.GEN_ID: gen_id,
            Gen.TYPE: type,
            Gen.PARAM: param,
            Gen.RESULT: result,
            Gen.DEPENDENT: dependent,
            Gen.USAGE: usage,
            Gen.MATERIALIZATION: materialization
        }):
            return gen_id
        else:
            return None

    def update_gen_result(self, tb_id):
        import util.view_generator.loader as view_generator
        gen_info = self._get_one({Gen.TB_ID: tb_id}, (Gen.GEN_ID, Gen.TYPE, Gen.PARAM))
        if gen_info:
            # todo: sql合表的自动更新有问题
            generator_info = json.loads(gen_info[Gen.PARAM])
            # 这个字段主要是给聚合表类型的合表使用，因为聚合表的字段id是随机生成的，需要根据之前生成的sql判断字段是否发生变化
            generator_info['current_tb_id'] = tb_id
            generator = view_generator.generators[gen_info[Gen.TYPE]]
            gen_tb_data = generator.generator_sql(generator_info)
            self._update({Gen.GEN_ID: gen_info[Gen.GEN_ID]}, {Gen.RESULT: gen_tb_data['sql']})
            # 返回gen_id是为了便于FE根据gen_id触发级联合表
            return gen_info[Gen.GEN_ID]

    def delete_by_tb(self, tb_id):
        self._delete({Gen.TB_ID: tb_id})

    def get_by_tb(self, tb_id, cond=()):
        return self._get_one({Gen.TB_ID: tb_id}, cond)

    def get_one(self, gen_id, cond=()):
        return self._get_one({Gen.GEN_ID: gen_id}, cond)

    def update(self, gen_id, param):
        return self._update({Gen.GEN_ID: gen_id}, param)

    def update_by_tb(self, tb_id, param):
        return self._update({Gen.TB_ID: tb_id}, param)
