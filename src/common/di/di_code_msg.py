#!/usr/bin/python
# -*- coding: utf-8 -*-

import common.di.di_error_code as DI_ERROR_CODE

UNKNOWN_ERROR = ' API内部错误，请联系BDP客服'


def get_msg(error_code):
    return msgs.get(error_code, UNKNOWN_ERROR)


msgs = {
    DI_ERROR_CODE.NORMAL: '正常',
    DI_ERROR_CODE.UNKNOWN_ERROR: '未知错误，请联系BDP客服处理',
    DI_ERROR_CODE.COOKIE_EXPIRED: '登录已失效，请重新登录',
    DI_ERROR_CODE.ACCESS_RIGHT_ERROR: '访问权限错误，请确认权限后再操作。',
    DI_ERROR_CODE.MISSING_ARGUMENT: '缺少参数',
    DI_ERROR_CODE.NO_FILE_UPLOAD: '没有上传文件，请重新上传。',
    DI_ERROR_CODE.UPLOAD_ERROR: '上传出错，请重新上传。',
    DI_ERROR_CODE.API_INTERNAL_ERROR: 'API内部错误，请联系BDP客服处理',

    DI_ERROR_CODE.JOB_NOT_EXISTS: '任务不存在，请检查后重新操作',
    DI_ERROR_CODE.PARAMETERS_INVALID: '参数无效，请检查后重新操作',
    DI_ERROR_CODE.IP_NOT_IN_SETTING_LIST: '登录IP不在登录许可列表，请在许可IP下访问。',
    DI_ERROR_CODE.EXCEL_FORMAT_ERROR: '文件格式错误，请重新操作',
    DI_ERROR_CODE.EXCEL_PARSE_ERROR: '文件解析错误，请重新操作',
    DI_ERROR_CODE.EXCEL_APPEND_ERROR: '文件追加错误，请重新操作',
    DI_ERROR_CODE.EXCEL_REPLACE_ERROR: '文件替换错误，请重新操作',
    DI_ERROR_CODE.EXCEL_DELETE_ERROR: '文件删除错误，请重新操作',
    DI_ERROR_CODE.EXCEL_NOT_EXISTS: '文件不存在，请重新操作',
    DI_ERROR_CODE.EXCEL_TB_SCHEMA_NOT_MATCH: '文件表头不匹配，请检查后重新操作',

    DI_ERROR_CODE.EXCEL_TB_OCCUPIED: '文件表头重复，请检查后重新操作',


    DI_ERROR_CODE.DB_CREATE_ERROR: '数据源创建错误，请重新操作',
    DI_ERROR_CODE.DS_DELETE_ERROR: '数据源删除错误，请重新操作',
    DI_ERROR_CODE.DS_NOT_EXISTS: '数据源不存在，请重新操作',
    DI_ERROR_CODE.DB_EXISTS: '数据源中有工作表，不能删除',
    DI_ERROR_CODE.DS_CONN_ERROR: '数据源连接错误,请确认连接信息后重新操作',
    DI_ERROR_CODE.UNSUPPORTED_DS_TYPE: '暂不支持该数据源类型',
    DI_ERROR_CODE.DS_SYNC_ERROR: '数据源同步错误，请检查数据源配置',

    DI_ERROR_CODE.TASK_EXEC_ERROR: '任务执行异常，请联系BDP客服',

    DI_ERROR_CODE.TB_INFO_ERROR: '工作表出错，请联系BDP客服',

    DI_ERROR_CODE.CAPACITY_OVERRUN_ERROR: '容量超限',

}

if __name__ == '__main__':
    print get_msg(DI_ERROR_CODE.API_INTERNAL_ERROR)
    print get_msg('sef')
