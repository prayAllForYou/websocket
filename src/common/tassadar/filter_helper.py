#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'stan'

OP_EQ = 0  # equal
OP_NE = 1  # not equal
OP_GT = 2  # greater than
OP_LT = 3  # less than
OP_GE = 4  # greater than or equal
OP_LE = 5  # less than or equal
OP_CONTAINS = 6
OP_NOT_CONTAIN = 7
OP_IS_EMPTY = 8
OP_IS_NOT_EMPTY = 9
OP_BETWEEN = 10

operator_map = {
    OP_EQ: '=',  # equal
    OP_NE: '<>',  # not equal
    OP_GT: '>',  # greater than
    OP_LT: '<',  # less than
    OP_GE: '>=',  # greater than or equal
    OP_LE: '<=',
    OP_CONTAINS: 'like',
    OP_NOT_CONTAIN: 'not like',
    OP_IS_EMPTY: 'is null',
    OP_IS_NOT_EMPTY: 'is not null',
    OP_BETWEEN: 'between'
}

condition_map = ['and', 'or']


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def make_condition(data_type, fid, operator, value, value_2nd=None):
    """
    将过滤规则转换为where条件
    :param data_type: number|string|date等
    :param fid: fkabcd1344
    :param operator: 0~10
    :param value:
    :return:
    """
    expand = None
    if data_type == 'number' and operator not in (OP_IS_EMPTY, OP_IS_NOT_EMPTY):
        if not is_number(value):
            raise Exception('数值筛选值%s不合法，请重新填写。' % value)
        expand = '%(fid)s %(oper)s %(value)s'
        if operator in (OP_EQ, OP_NE):
            expand = "%(fid)s %(oper)s '%(value)s'"
    else:
        expand = "%(fid)s %(oper)s '%(value)s'"
    if operator == OP_NE:
        expand = '(' + expand + ' or %(fid)s is null)'
    elif operator == OP_CONTAINS:
        expand = "%(fid)s %(oper)s '%%%(value)s%%'"
    elif operator == OP_NOT_CONTAIN:
        expand = "(%(fid)s %(oper)s '%%%(value)s%%' or %(fid)s is null)"
    elif operator == OP_IS_EMPTY:
        expand = "%(fid)s %(oper)s or %(fid)s = ''"
    elif operator == OP_IS_NOT_EMPTY:
        expand = "%(fid)s %(oper)s and cast(%(fid)s as string) <> ''"
    elif operator == OP_BETWEEN:
        expand = "%(fid)s %(oper)s '%(value)s' and '%(value_2nd)s'"

    expand = expand % {'fid': fid, 'oper': operator_map[operator], 'value': value, 'value_2nd': value_2nd}

    return "(%s)" % expand
