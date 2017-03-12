#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

import re
from common.logger import Logger
from metabase import MetaBase
from share import Share
from field_relation import FieldRelation


class VField(MetaBase, Logger):
    """
    计算字段，虚拟字段
    """
    _table = 'VFIELD'
    """字段定义"""
    TB_ID = 'tb_id'
    FIELD_ID = 'field_id'
    SEQ_NO = 'seq_no'
    NAME = 'name'
    TITLE = 'title'
    TYPE = 'type'
    REMARK = 'remark'
    CTIME = 'ctime'
    UTIME = 'utime'
    AGGREGATOR = 'aggregator'
    # ROW_AGGREGATOR = 'row_aggregator'
    PARAM = 'param'
    OWNER = 'owner'
    FLAG = 'flag'

    # 把计算字段解开以后的公式
    RAW_FORMULA = 'raw_formula'

    # flag标记定义
    CHART_VFIELD_FLAG = 0   # 用于作图的计算字段
    TABLE_VFIELD_FLAG = 1   # 用于工作表的计算字段
    """字段类型定义(修改后注意调整to_mobius_schema的mobius_type_map的匹配关系)"""
    INT_TYPE = 0
    DOUBLE_TYPE = 1
    STRING_TYPE = 2
    DATETIME_TYPE = 3

    DATETIME_GROUP_FIELD = 'fixed_real_time'

    # 跟Field里的一样，这里不能引入field
    FIELD_PATTERN = re.compile('fk[a-f0-9]{8}')

    def create(self, params):
        required_params = (VField.NAME, VField.TYPE, VField.AGGREGATOR)
        for rp in required_params:
            if rp not in params:
                self.logger.warn('缺失字段：%s' % rp)
                return None
        if not params.get(VField.FIELD_ID, ''):
            from field import create_field_id
            params[VField.FIELD_ID] = create_field_id()
        # use string as default type
        if self._create(params):
            return params[VField.FIELD_ID]
        else:
            return None

    def get_list(self, tb_id, user_id, cols=(), offset=0, limit=1000):
        fields = self.get({VField.TB_ID: tb_id, VField.OWNER: user_id}, cols, offset, limit, order=VField.SEQ_NO)
        return fields

    def get_one(self, tb_id, field_id, cols=()):
        field = self._get_one({VField.TB_ID: tb_id, VField.FIELD_ID: field_id}, cols)
        return field

    def update(self, tb_id, field_id, params, is_del=False):
        return self._update({VField.TB_ID: tb_id, VField.FIELD_ID: field_id, 'is_del': is_del}, params)

    def delete(self, tb_id, field_id):
        return self._delete({VField.TB_ID: tb_id, VField.FIELD_ID: field_id})

    # 搁置，待完成
    def add_with_share(self, params):
        field_id = self.create(params)
        tb_or_share_id = params[self.TB_ID]
        share_model = Share()
        shares = share_model.get({Share.TB_ID: tb_or_share_id}, (Share.SH_ID, Share.COL_FILTER))
        for share in shares:
            col_filter = json.loads(share[Share.COL_FILTER])
            col_filter.append(field_id)

    def sort_vfield_refer_by_refer(self, vfields):
        refer_fids_list = self.gen_refer_fids_list(vfields)
        sorted_vfield_ids = self.sort_by_appear_seq(refer_fids_list)
        return sorted_vfield_ids

    def gen_refer_fid_dict(self, vfields):
        '''
        create vfid_refer_fid vfid:fid,fid
        '''
        refer_fid_dict = {}
        vfids = [f[VField.FIELD_ID] for f in vfields]
        for vfield in vfields:
            vfield_id = vfield[VField.FIELD_ID]
            aggr = vfield[VField.AGGREGATOR]
            refer_fids = VField.FIELD_PATTERN.findall(aggr)
            new_fids = []
            for refer_fid in refer_fids:
                if refer_fid in vfids:
                    new_fids.append(refer_fid)
            refer_fid_dict[vfield_id] = new_fids
        return refer_fid_dict

    def sort_by_appear_seq(self, refer_fid_dict):
        if not refer_fid_dict:
            return []
        refer_list = []
        print refer_fid_dict
        for key, value in refer_fid_dict.items():
            if not value:
                if key not in refer_list:
                    refer_list.append(key)
                continue
            self._sort_fid(key, refer_fid_dict, refer_list, 1)
        return refer_list

    def _sort_fid(self, fid, refer_fid_dict, refer_list, deep):
        if deep > 1000:
            raise Exception('循环依赖')
        values = refer_fid_dict[fid]
        if values:
            for value in values:
                self._sort_fid(value, refer_fid_dict, refer_list, deep + 1)
        if fid not in refer_list:
            refer_list.append(fid)
        return

    def sync(self, old_tb, new_tb, keep_fid=False):
        """
        判断目标表中是否缺少所需的计算字段，如果缺失则自动创建
        :param old_tb:
        :param new_tb:
        :return:
        """
        from tb import TB
        from field import Field, create_field_id

        old_fields = sorted(old_tb[TB.FIELDS], key=lambda x: x.get(VField.SEQ_NO))
        new_fields = new_tb[TB.FIELDS]
        id_mapper = {}
        # todo: 将来字段改为引用的形式，这里也需要调整
        for old_f in [f for f in old_fields]:
            for new_f in [f for f in new_fields]:
                if old_f[Field.TITLE] == new_f[Field.TITLE]:
                    id_mapper[old_f[Field.FIELD_ID]] = new_f[Field.FIELD_ID]

        vfields = [f for f in old_tb[TB.FIELDS] if VField.FLAG in f]
        vfield_dict = {}
        for vfield in vfields:
            vfield_dict[vfield[VField.FIELD_ID]] = vfield
        refer_fid_dict = self.gen_refer_fid_dict(vfields)
        _sorted_fid = self.sort_by_appear_seq(refer_fid_dict)

        # 同步计算字段，只同步表的拥有者的计算字段
        # for current_f in [f for f in old_fields if VField.FLAG in f and f[VField.OWNER] == old_tb[TB.OWNER]]:
        for vfid in _sorted_fid:
            current_f = vfield_dict[vfid]
            vfield_exists = False
            for f in new_fields:
                if f[Field.TITLE] == current_f[Field.TITLE]:
                    vfield_exists = True
                    break
            # 计算字段在目标表中不存在，需要按照旧的公式生成计算字段
            if not vfield_exists:
                aggr = current_f[VField.AGGREGATOR]
                # 替换字段
                for f in Field.FIELD_PATTERN.findall(aggr):
                    if f not in id_mapper:
                        raise Exception('sync aggregator error: %s -> %s, current_field: %s, old fid: %s title mismatch\r' %
                                        (old_tb[TB.TB_ID], new_tb[TB.TB_ID], current_f[Field.TITLE], f))
                    aggr = aggr.replace(f, id_mapper[f])

                # 替换编辑参数
                param = current_f[VField.PARAM]
                if param:
                    for f in Field.FIELD_PATTERN.findall(param):
                        if f not in id_mapper:
                            raise Exception('sync param error: %s -> %s, old fid: %s title mismatch, current_f: %s' %
                                            (old_tb[TB.TB_ID], new_tb[TB.TB_ID], f, current_f))
                        param = param.replace(f, id_mapper[f])
                new_aggr_field = {
                    VField.TB_ID: new_tb[TB.TB_ID],
                    VField.FIELD_ID: current_f[VField.FIELD_ID] if keep_fid else create_field_id(),
                    VField.AGGREGATOR: aggr,
                    VField.OWNER: new_tb[TB.OWNER],
                    VField.TYPE: current_f[VField.TYPE],
                    VField.FLAG: current_f[VField.FLAG],
                    VField.TITLE: current_f[VField.TITLE],
                    VField.NAME: current_f[VField.NAME],
                    VField.SEQ_NO: current_f[VField.SEQ_NO],
                    VField.PARAM: param
                }
                # 保存新增计算字段到数据库
                self.create(new_aggr_field)
                build_relation(new_aggr_field)
                # 直接将新增字段附加到new_tb对象中，后续直接使用
                new_tb[TB.FIELDS].append(new_aggr_field)
                # 更新id_mapper
                id_mapper[current_f[VField.FIELD_ID]] = new_aggr_field[VField.FIELD_ID]


def is_group_field(field_info):
    if VField.PARAM in field_info and field_info[VField.PARAM]:
        param = json.loads(field_info[VField.PARAM])
        if param.get('type') == 'group':
            return True
    return False


def build_relation(field_info):
    fr = FieldRelation()
    if is_group_field(field_info):
        # 分组字段
        param = json.loads(field_info[VField.PARAM])
        dep = param.get('fid')
        if dep:
            fr.create(field_info[VField.TB_ID], field_info[VField.FIELD_ID], field_info[VField.TB_ID], dep)
            return True
    deps = set(VField.FIELD_PATTERN.findall(field_info[VField.AGGREGATOR]))
    for f in deps:
        fr.create(field_info[VField.TB_ID], field_info[VField.FIELD_ID], field_info[VField.TB_ID], f)
    return True
