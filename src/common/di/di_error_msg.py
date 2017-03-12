#!/usr/bin/python
# encoding:utf-8
"""
tassadar api ERROR_MSG Constants
"""
# code=12, 不要用

# global
EMPTY_ERROR = ''

# excel util/excel_parser
FIRST_LINE_IS_NONE = '工作表为空'
NONE_TITLE_IN_SCHEMA = '表头中不能包含空列,空列位置:%s列'
NONE_DATA_IN_SCHEMA = '工作表无数据'
SAME_TITLE_IN_SCHEMA = '表头中包含%s个重复字段:%s'

CSV_FILE_ENCODE_NOT_SUPPORT = '不支持的CSV文件编码格式'
CSV_FILE_ENCODE_SUPPORT_UTF8_GBK = '目前仅支持UTF8和GBK编码，请将您的文件编码进行转换'
