#!/usr/bin/python
# -*- coding: utf-8 -*-

import common.tassadar.error_code as ERROR_CODE

UNKNOWN_ERROR = ' 未知错误'


def get_msg(error_code):
    return msgs.get(error_code, UNKNOWN_ERROR)


msgs = {
    # global,
    ERROR_CODE.NORMAL: '正常',
    ERROR_CODE.API_INTERNAL_ERROR: 'API内部错误,请联系BDP客服',
    ERROR_CODE.ACCESS_RIGHT_ERROR: '访问权限错误，请确认权限后再操作',
    ERROR_CODE.ILLEGAL_ARGUMENT_ERROR: '参数不正确',
    ERROR_CODE.DATA_NOT_EXISTS: '数据不存在',
    ERROR_CODE.PERMISSION_DENIED: '权限不足',

    # db,
    ERROR_CODE.DB_NOT_EXISTS: '数据源不存在，请重新操作',

    # tb,
    ERROR_CODE.TB_NOT_EXISTS: '工作表不存在，请重新操作',
    ERROR_CODE.TB_USING: '工作表被使用',
    ERROR_CODE.TB_MERGE_ERROR: '合表失败，请检查后重新操作',

    ERROR_CODE.TB_IS_BASE_TABLE: '工作表为基础表',

    # view,
    ERROR_CODE.VIEW_RELATION_ERROR: '合表关联异常',
    ERROR_CODE.UNION_FIELD_NAME_DUPLICATED: '追加合并存在重复列',

    # field,
    ERROR_CODE.FIELD_NOT_EXISTS: '字段不存在',
    ERROR_CODE.FIELD_NO_PERMISSION: '权限不足',
    ERROR_CODE.FIELD_SAME_NAME: '字段重复',
    ERROR_CODE.FIELD_NAME_NOT_MATCH: '字段不存在',

    # task,
    ERROR_CODE.TASK_NOT_EXISTS: '任务不存在',

    # ds,
    ERROR_CODE.DS_NOT_EXISTS: '数据源不存在',

    # share,
    ERROR_CODE.SHARE_NOT_EXISTS: '分享表不存在，请检查后重新操作',
}

if __name__ == '__main__':
    print get_msg(ERROR_CODE.API_INTERNAL_ERROR)
    print get_msg('sef')
