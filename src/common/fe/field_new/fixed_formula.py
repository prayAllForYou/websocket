#!/usr/bin/python
# -*- coding: utf-8 -*-

import json

from common.tassadar.meta.vfield import VField
from common.tassadar.client import TassadarClient
from common.fe.field_new.group_formula_builder import GroupFormulaBuilder
from traceback import print_exc


class FixedFormula(object):

    def get_fixed_formula(self, tb_id, field_info, locale_code = "zh"):
        # 调用tassadar client 取得相应的最大值
        user_id = field_info[VField.OWNER]
        try:
            tsd = self._get_query_client()
            param = json.loads(field_info['param'])
            aggregator = param['info']['aggregator']
            fid_formula = param.get("fid_formula", "") if param.get("fid_formula", "") else param.get("fid", "")
            if aggregator == "year":
                fields = ['YEAR(MAX(%s))' % fid_formula, ]
                res = tsd.query_tb(tb_id, user_id, fields=fields, )
                end = [int(res['data'][0][0]),]
            elif aggregator == "quarter":
                fields = ['YEAR(MAX(%s))' % fid_formula, 'QUARTER(MAX(%s))' % fid_formula]
                res = tsd.query_tb(tb_id, user_id, fields=fields, )
                end = [int(res['data'][0][0]), int(res['data'][0][1])]
            elif aggregator == "month":
                fields = ['YEAR(MAX(%s))' % fid_formula, 'MONTH(MAX(%s))' % fid_formula]
                res = tsd.query_tb(tb_id, user_id, fields=fields, )
                end = [int(res['data'][0][0]), int(res['data'][0][1])]
            elif aggregator == "week":
                fields = ['YEAR(MAX(%s))' % fid_formula, 'WEEK(MAX(%s))' % fid_formula]
                res = tsd.query_tb(tb_id, user_id, fields=fields)
                end = [int(res['data'][0][0]), int(res['data'][0][1].split(" ")[1])]
            elif aggregator == "day":
                fields = ['MAX(%s)' % fid_formula]
                res = tsd.query_tb(tb_id, user_id, fields=fields)
                end = [res['data'][0][0].split(" ")[0],]
            param['info']['end'] = end
            param['info']['type'] = 'fixed_real_time'
            return GroupFormulaBuilder(param, locale_code).build()
        except Exception as e:
            print_exc()
            return ''

    def _get_query_client(self):
        return TassadarClient()

