#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
from functools import partial
import json
from common.mobius import Mobius
from common.tassadar.meta.tb import TB
from common.tassadar.meta.field import Field, filter_vfields_by_flag, expand_vfield
from common.tassadar.meta.field import get_invalid_fields
from common.tassadar.meta.vfield import VField
from common.tassadar.meta.share import Share
import common.error_codes as error_code
_backslash = re.compile(r'\\[^u\'"abntvrfox0\\]')   # 匹配各种转义字符
_datetime_partten = re.compile(r"'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'")
__author__ = 'stan'


def _replace_with_formula(datetime, formula, base_field):
    if not isinstance(datetime, basestring):
        datetime = datetime.group(0)
    return formula.replace(base_field, datetime)


def append_partition_filter(tb_info, where_info):
    """
    根据where条件附加分区信息
    :param tb_info:     必须包含partition和version字段
    :param where_info:
    :return:
    """
    if not where_info:
        return
    partition = json.loads(tb_info[TB.PARTITION]) if tb_info[TB.PARTITION] else None
    # 如果设置分区时的版本和工作表的版本一样，说明表还没有更新，此时partition是无效的，查询时不应该引入分区字段
    if partition and partition['set_version'] == tb_info[TB.VERSION]:
        partition = None
    if not partition:
        return
    # 纯文本字段，直接进行替换
    base_field = partition['base_field']
    formula = partition['formula']
    if base_field == formula:
        for i, w in enumerate(where_info):
            where_info[i] = w.replace(base_field, 'pk')
    else:
        partition_where = []
        for i, w in enumerate(where_info):
            # 目前限定基于日期的分区字段，只处理形如这种格式的：fkffdb641b >= '2016-04-06 00:00:00' AND fkffdb641b <= '2016-04-06 23:59:59'
            if len(w) == 75 and base_field in w:
                r_func = partial(_replace_with_formula, formula=formula, base_field=base_field)
                result = _datetime_partten.sub(r_func, w)
                around_formula = result.replace(base_field, 'pk')
                partition_filter = around_formula.replace('> ', '>= ').replace('< ', '<= ')
                partition_where.append(partition_filter)
        if partition_where:
            where_info = partition_where + where_info
    return where_info


def query(user_id, tb_id, fields, where_info=None, group_info=None, order_by='', limit=50):
    mobius = Mobius()
    sql_result = get_sql(user_id, tb_id, fields, where_info, group_info, order_by, limit)
    if sql_result['status'] != error_code.NORMAL:
        raise Exception(sql_result['result'])

    sql = sql_result['result']
    result = mobius.query(sql)
    return result['data']


def get_sql(user_id, tb_id, fields, where_info=None, group_info=None, order_by='', limit=50):
    if not group_info:
        group_info = []
    if not where_info:
        where_info = []
    is_share_tb = True if tb_id.find('sh_') == 0 else False
    tb_model = TB()

    tb_info = tb_model.get_tb(tb_id, (TB.OWNER, TB.STORAGE_ID, TB.DATA_COUNT), user_id=user_id)
    if tb_info is None:
        return {
            'status': error_code.TB_NOT_EXISTS,
            'result': '查询的表不存在：%s' % tb_id
        }

    # 如果不是此表的owner，从share表中获取fields和row_filter的信息进行组合
    if is_share_tb:
        # 分享表需要对访问权限进行限制
        valid_fields = [f[Field.FIELD_ID] for f in tb_info[TB.FIELDS]]
        invalid_fields = get_invalid_fields(valid_fields, fields)
        if len(invalid_fields) != 0:
            invalid_field_titles = [name[Field.TITLE] for name in tb_info[TB.FIELDS]
                                    if name[Field.FIELD_ID] in invalid_fields]
            if invalid_field_titles:
                error_info = '存在无权访问的字段：%s' % json.dumps(invalid_field_titles)
            else:
                error_info = '访问的字段不存在：%s' % json.dumps(invalid_fields)
            return {
                'status': error_code.PERMISSION_DENIED,
                'result': error_info
            }
        # 如果是分享表，获取tb_info的时候会自动添加行过滤条件
        for where in tb_info[Share.ROW_FILTER]:
            where_info.append(where)

    storage_id = tb_info[TB.STORAGE_ID]
    # 拆解计算字段, FE已做了一层拆解，这里只做工作表级别的字段拆解
    vfields = filter_vfields_by_flag(tb_info[TB.FIELDS], VField.TABLE_VFIELD_FLAG)
    fields = expand_vfield(fields, vfields)
    where_info = expand_vfield(where_info, vfields)
    group_info = expand_vfield(group_info, vfields)

    where_info = [w for w in where_info if w]
    sql = __make_sql(storage_id, fields, where_info, group_info, order_by, limit)
    # 处理转义字符，SparkSQL还不能直接处理类似 '类型\乳制品' 这样的字符串，尼玛，给转义了
    sql = _backslash.sub(__append_backslash, sql)
    return {
        'status': error_code.NORMAL,
        'result': sql,
        'tb_info': tb_info
    }


def __append_backslash(m):
    return '\\%s' % m.group(0)


def __make_sql(storage_id, fields, where_info, group_info, order_by, limit):
    fields_clause = ' * '
    if fields:
        fields_clause = ','.join(fields)
    where_clause = ''
    if where_info:
        where_clause = 'WHERE '
        where_clause += ' AND '.join(where_info)
    group_clause = ''
    if group_info:
        group_clause = 'GROUP BY '
        group_clause += ','.join(group_info)
    limit_clause = ''
    # 数据查询最大允许数据量为10W
    if limit:
        if int(limit) > 100000:
            limit = 100000
        limit_clause = 'LIMIT %s' % limit
    order_by_clause = ''
    if order_by:
        order_by_clause = 'ORDER BY %s' % order_by

    sql = 'SELECT %s FROM %s %s %s %s %s' % (fields_clause, storage_id, where_clause,
                                             group_clause, order_by_clause, limit_clause)
    return sql
