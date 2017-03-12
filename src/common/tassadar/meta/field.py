#!/usr/bin/env python
# -*- coding: utf-8 -*-
import hashlib
from functools import partial
import re
import json

from share import Share
from vfield import VField
from common.logger import Logger
from metabase import MetaBase
import common.tools as tools
from common.tassadar.meta.field_relation import FieldRelation


class Field(MetaBase, Logger):
    _table = 'FIELD'
    """字段定义"""
    TB_ID = 'tb_id'
    FIELD_ID = 'field_id'
    SEQ_NO = 'seq_no'
    NAME = 'name'
    TITLE = 'title'
    TYPE = 'type'
    UNIQ_INDEX = 'uniq_index'
    CTIME = 'ctime'
    UTIME = 'utime'
    REMARK = 'remark'
    """字段类型定义(修改后注意调整to_mobius_schema的mobius_type_map的匹配关系)"""
    INT_TYPE = 0
    DOUBLE_TYPE = 1
    STRING_TYPE = 2
    DATETIME_TYPE = 3
    NULL_DATA_TYPE = 999
    # 用于匹配字段id的正则表达式
    FIELD_PATTERN = re.compile('fk[a-f0-9]{8}')

    def create(self, params):
        required_params = (Field.NAME, Field.TYPE)
        for rp in required_params:
            if rp not in params:
                self.logger.warn('缺失字段：%s' % rp)
                return None
        if params[Field.TYPE] == Field.NULL_DATA_TYPE:
            params[Field.TYPE] = Field.STRING_TYPE
        if not params.get(Field.FIELD_ID, ''):
            params[Field.FIELD_ID] = create_field_id()
        # use string as default type
        if self._create(params):
            return params[Field.FIELD_ID]
        else:
            return None

    # 添加字段的同时将字段添加到分享表中(满足当前需求：不对字段的访问权限进行控制)
    def add_with_share(self, params):
        required_params = (Field.NAME, Field.TYPE)
        for rp in required_params:
            if rp not in params:
                self.logger.warn('缺失字段：%s' % rp)
                return None
        if not params.get(Field.FIELD_ID, ''):
            params[Field.FIELD_ID] = create_field_id()
        if params[Field.TYPE] == Field.NULL_DATA_TYPE:
            params[Field.TYPE] = Field.STRING_TYPE
        fid = params[Field.FIELD_ID]
        if self._create(params):
            tb_id = params[Field.TB_ID]
            share_model = Share()
            shares = share_model.get({Share.TB_ID: tb_id}, (Share.SH_ID, Share.COL_FILTER))
            for share in shares:
                col_filter = json.loads(share[Share.COL_FILTER])
                col_filter.append(fid)
                share_model.update(share[Share.SH_ID], {Share.COL_FILTER: col_filter})
            return fid
        else:
            return None

    def get_list(self, tb_id, cols=(), offset=0, limit=1000):
        fields = self.get({Field.TB_ID: tb_id}, cols, offset, limit, order=Field.SEQ_NO)
        return fields

    def get_one(self, tb_id, field_id, cols=()):
        field = self._get_one({Field.TB_ID: tb_id, Field.FIELD_ID: field_id}, cols)
        return field

    def update(self, tb_id, field_id, params, is_del=False):
        return self._update({Field.TB_ID: tb_id, Field.FIELD_ID: field_id, 'is_del': is_del}, params)

    def delete_with_share(self, tb_id, field_id):
        """
        删除字段，并从分配表中也将分配的字段移除
        """
        result = self._delete({Field.TB_ID: tb_id, Field.FIELD_ID: field_id})
        if result:
            share_model = Share()
            shares = share_model.get({Share.TB_ID: tb_id}, (Share.SH_ID, Share.COL_FILTER))
            for share_info in shares:
                share_info[Share.COL_FILTER] = json.loads(share_info[Share.COL_FILTER])
                if field_id in share_info[Share.COL_FILTER]:
                    share_info[Share.COL_FILTER].remove(field_id)
                share_model._update({Share.SH_ID: share_info[Share.SH_ID]},
                                    {Share.COL_FILTER: share_info[Share.COL_FILTER]})
        return result


def create_field_id(base_info=None):
    """
    创建field_id, 使用k符号，是因为OLAP使用了f_这样的日期表达方式，这里使用f_和导致冲突
    :return:
    """
    key = tools.uniq_id()
    if base_info:
        key = hashlib.md5(base_info.encode('utf-8')).hexdigest()
    return 'fk%s' % key[0:8]


# 数据库类型和系统定义的数据类型的匹配关系
type_mapper = {
    "datetime": Field.DATETIME_TYPE,
    "date": Field.DATETIME_TYPE,
    "time": Field.DATETIME_TYPE,
    "timestamp": Field.DATETIME_TYPE,
    "int": Field.INT_TYPE,
    "long": Field.INT_TYPE,
    "tinyint": Field.INT_TYPE,
    "smallint": Field.INT_TYPE,
    "bigint": Field.INT_TYPE,
    "float": Field.DOUBLE_TYPE,
    "double": Field.DOUBLE_TYPE,
    "decimal": Field.INT_TYPE,
    "string": Field.STRING_TYPE,
    "number": Field.DOUBLE_TYPE
}


def to_mobius_schema(fields):
    """
    辅助方法，将字段信息转换为mobius所需的字段信息
    :param fields:
    :return:
    """
    mobius_type_map = ('integer', 'double', 'string', 'date')
    schema = []
    for field in fields:
        type_index = int(field['type'])
        type_index = 2 if type_index >= len(mobius_type_map) else type_index
        schema.append({
            'name': field[Field.FIELD_ID],
            'seq_no': field[Field.SEQ_NO],
            # 类型转换
            'type': mobius_type_map[type_index]
        })
    return schema


# 暂时不用了，FE会根据tb/info确定字段信息（之前是在tb/query的地方用）
def ship_field_info(schema, tb_fields):
    """
    辅助方法，为mobius中db/query返回的schema信息添加name，title信息
    :param schema:
    :param tb_fields:
    :return:
    """
    for field in schema:
        field[Field.FIELD_ID] = field['name']
        field[Field.NAME] = field['name']
        field[Field.TITLE] = field['name']
        field[Field.TYPE] = type_mapper.get(field[Field.TYPE], Field.STRING_TYPE)
        for f in tb_fields:
            if f[Field.FIELD_ID] == field['name']:
                field['name'] = f[Field.NAME]
                field['title'] = f[Field.TITLE]
                field['seq_no'] = f[Field.SEQ_NO]
                field['type'] = f[Field.TYPE]
                if VField.FLAG in f:
                    field[VField.FLAG] = f[VField.FLAG]
                if VField.ROW_AGGREGATOR in f:
                    field[VField.ROW_AGGREGATOR] = f[VField.ROW_AGGREGATOR]
                break
        if Field.SEQ_NO not in field:
            field['seq_no'] = len(tb_fields)


def get_invalid_fields(valid_fields, select_fields):
    """
    获取用户无权使用的字段列表
    :param valid_fields:用户可使用的字段列表
    :param select_fields:前台传入的查询信息列表
    :return:
    """
    invalid_fields = []
    for select_field in select_fields:
        # select_field有可能是类似这样的形式：fun1(fun2(f_01238729, f_acfb0122), f_abc12189)
        for field in Field.FIELD_PATTERN.findall(select_field):
            if field not in valid_fields:
                invalid_fields.append(field)
    return list(set(invalid_fields))


def filter_vfields_by_flag(fields, flag=VField.TABLE_VFIELD_FLAG):
    return [f for f in fields if VField.FLAG in f and f[VField.FLAG] == flag]


def filter_vfields(fields):
    return [f for f in fields if VField.FLAG in f]


# 将fields中的普通字段,工作表计算字段, 图表计算字段分三个数组返回
def split_fields_by_flag(fields):
    regular_fields = []
    table_vfields = []
    for f in fields:
        if VField.FLAG not in f:
            regular_fields.append(f)
        elif f[VField.FLAG] == VField.TABLE_VFIELD_FLAG:
            table_vfields.append(f)

    return regular_fields, table_vfields

# fields:表的所有字段,
# nfields:需要用到的字段
def split_fields_by_flag1(fields, nfields):
    regular_fields = []
    table_vfields = []
    for f in fields:
        if VField.FLAG in f:
            if f[VField.FLAG] == VField.TABLE_VFIELD_FLAG:
                table_vfields.append(f)
        elif len(nfields) != 0 and f[Field.FIELD_ID] not in nfields:
            continue
        else:
            regular_fields.append(f[Field.FIELD_ID])

    return regular_fields, table_vfields


def _replace_field_with_storage_id(fid, storage_id):
    if not isinstance(fid, basestring):
        fid = fid.group(0)
    return '`%s`.`%s`' % (storage_id, fid)


def expand_vfield_old(field_or_list, vfields, storage_id=None):
    """
    将字段中的field_id拆解为计算字段所表示的公式
    :param field_or_list: 字段，或字段列表， 如fkabdd1234, 或['fkabdd1234', 'fkaedc5879>10']
    :param vfields:
    :param storage_id: 如果指定了storage_id, 那么返回的字段信息中会附加上storage_id
    :return:
    """
    if not vfields and not storage_id:
        return field_or_list

    is_str = type(field_or_list) is not list
    new_field_list = None
    if is_str:
        new_field_list = [field_or_list]
    else:
        import copy
        new_field_list = copy.deepcopy(field_or_list)

    for i, field in enumerate(new_field_list):
        for f in Field.FIELD_PATTERN.findall(field):
            for vfield in vfields:
                if vfield[Field.FIELD_ID] == f:
                    aggr = vfield[VField.AGGREGATOR] if VField.AGGREGATOR in vfield else vfield[VField.ROW_AGGREGATOR]
                    # 将计算字段id拆解为表达式
                    field = field.replace(f, aggr)
                    break
        if storage_id:
            r_func = partial(_replace_field_with_storage_id, storage_id=storage_id)
            field = Field.FIELD_PATTERN.sub(r_func, field)
        new_field_list[i] = field
    # 如果传的是单个字段，则返回单个字段信息
    if is_str:
        return new_field_list[0]
    return new_field_list


def expand_vfield(field_or_list, vfields, storage_id=None):
    """ 递归实现展开计算公式
    """
    if not vfields and not storage_id:
        return field_or_list

    vfield_map = {}
    for f in vfields:
        vfield_map[f[Field.FIELD_ID]] = f

    is_str = not isinstance(field_or_list, (list, tuple))
    new_field_list = None
    if is_str:
        new_field_list = [field_or_list]
    else:
        import copy
        new_field_list = copy.deepcopy(field_or_list)

    for i, field in enumerate(new_field_list):
        expand_level = 0
        while True:
            expand_level += 1
            res = set(Field.FIELD_PATTERN.findall(field)) & set(vfield_map.keys())
            if expand_level > 20:
                dup_fields = [vfield_map[r][Field.TITLE] for r in res]
                raise Exception("计算字段循环依赖: %s" % ','.join(dup_fields))
            if not len(res):
                break
            for f in res:
                vfield = vfield_map[f]
                aggr = vfield[VField.AGGREGATOR] if VField.AGGREGATOR in vfield else vfield[VField.ROW_AGGREGATOR]
                # 将计算字段id拆解为表达式
                field = field.replace(f, '(%s)' % aggr)
        if storage_id:
            r_func = partial(_replace_field_with_storage_id, storage_id=storage_id)
            field = Field.FIELD_PATTERN.sub(r_func, field)
        new_field_list[i] = field
    # 如果传的是单个字段，则返回单个字段信息
    if is_str:
        return new_field_list[0]
    return new_field_list


def expand_single_vfield(tb_id, field, vfields):
    """ 根据字段的依赖关系展开计算字段， 只解开一层
    """
    fr = FieldRelation()
    deps = fr.get_dep_fields(field[Field.TB_ID], field[Field.FIELD_ID])
    for dep in deps:
        if dep[FieldRelation.DEP_FIELD_ID] in vfields:
            dep_info = vfields[dep[FieldRelation.DEP_FIELD_ID]]
            aggr = '(%s)' % dep_info[VField.AGGREGATOR]
            field[VField.AGGREGATOR] = field[VField.AGGREGATOR].replace(dep[FieldRelation.DEP_FIELD_ID], aggr)
    return field


def get_seq_no(fields):
    max_seq_no = 0
    for f in fields:
        if f[Field.SEQ_NO] > max_seq_no:
            max_seq_no = f[Field.SEQ_NO]
    return max_seq_no + 1
