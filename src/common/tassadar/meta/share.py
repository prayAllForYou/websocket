#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import common.tools as tools
from common.tassadar.meta.metabase import MetaBase
from common.overlord.client import OverlordClient

MAX_SHARE_COUNT = 100

class Share(MetaBase):
    _table = 'SHARED_TB'
    """字段定义"""
    SH_ID = 'sh_id'
    TB_ID = 'tb_id'
    DEP_ID = 'dep_id'
    SHARER = 'sharer'
    TO_USER = 'to_user'
    TO_GROUP = 'to_group'
    # row_filter，只记录在当前表上设置的过滤条件，如果此分享表是在分享表的基础上创建的，get_one方法会自动将父表的过滤条件附加上
    # 并替换掉row_filter
    ROW_FILTER = 'row_filter'
    COL_FILTER = 'col_filter'
    EDITABLE = 'editable'
    PARAM = 'param'
    DATA_COUNT = 'data_count'
    VERSION = 'version'
    CTIME = 'ctime'
    UTIME = 'utime'
    IS_FIXED = 'is_fixed'
    """附加字段，不做存储"""
    FIELDS = 'fields'
    ORIGIN_ROW_FILTER = 'origin_row_filter'

    def create(self, params):
        # row_filter是一组WHERE条件,在多级分享中,row_filter通过AND方式叠加,可缺省
        # col_filter以白名单的方式运作,下一级能看到的col总是上一级的子集,不可缺省
        required_params = (Share.SHARER, Share.TO_USER, Share.TO_GROUP)
        for rp in required_params:
            if rp not in params:
                self.logger.warn('缺失字段：%s' % rp)
                return None

        params[Share.SH_ID] = tools.uniq_id('sh')
        if self._create(params):
            return params[Share.SH_ID]
        else:
            return None

    def delete(self, sh_id):
        self._delete({Share.SH_ID: sh_id})

    def get_one(self, sh_id, cols=(), sh_links=None, filter_delete=True):
        """
        获取分享表信息，已将row_filter处理（融入父分享表的过滤条件）
        :param sh_id:
        :param cols:
        :param sh_links: 如果调用者已经获取到分享链条信息，内部就不用每次获取了
        :param filter_delete:
        :return:
        """
        fields = list(cols)
        if fields and Share.ROW_FILTER in fields:
            if Share.DEP_ID not in fields:
                fields.append(Share.DEP_ID)
            if Share.TB_ID not in fields:
                fields.append(Share.TB_ID)
        share_conds = (Share.SH_ID, Share.DEP_ID, Share.ROW_FILTER)
        res = self._get_one({Share.SH_ID: sh_id}, fields, filter_delete)
        if res and Share.ROW_FILTER in res:
            tb_id = res[Share.TB_ID]
            share_list = [res]
            if res[Share.DEP_ID]:
                if not sh_links:
                    sh_links = self.get({Share.TB_ID: tb_id}, share_conds)
                self.find_parent_sh(sh_links, res[Share.DEP_ID], share_list)

            row_filters = []
            for sh in share_list:
                row_filters += json.loads(sh[Share.ROW_FILTER])
            res[Share.ORIGIN_ROW_FILTER] = json.loads(res[Share.ROW_FILTER])
            res[Share.ROW_FILTER] = row_filters
        return res

    def find_parent_sh(self, sh_links, sh_id, result):
        # todo: 当分表的数量特别多的时候，此处会有性能问题
        for sh in sh_links:
            if sh[Share.SH_ID] == sh_id:
                result.append(sh)
                if sh[Share.DEP_ID]:
                    self.find_parent_sh(sh_links, sh[Share.DEP_ID], result)
                break

    def get_one_with_conds(self, conds, cols=(), filter_delete=True):
        res = self._get_one(conds, cols, filter_delete)
        return res

    def get_one_with_conds(self, conds, cols=(), filter_delete=True):
        res = self._get_one(conds, cols, filter_delete)
        return res

    def get_list(self, to_user, sharer=None, cols=(), offset=0, limit=1000):
        conds = {}
        if sharer:
            conds[Share.SHARER] = sharer
        if to_user:
            conds[Share.TO_USER] = to_user
        if conds:
            return self.get(conds, cols, offset, limit)
        return []

    def update(self, sh_id, params):
        return self._update({Share.SH_ID: sh_id}, params)

    def get_rel_sh_tbs(self, sh_id, result):
        """
        递归获取所有依赖此分享表的分享表id列表
        :param sh_id:
        :param result:
        :return:
        """
        rel_tbs = self.get({Share.DEP_ID: sh_id}, (Share.SH_ID, ))
        for rel_sh in rel_tbs:
            rel_sh_id = rel_sh[Share.SH_ID]
            result.append(rel_sh_id)
            self.get_rel_sh_tbs(rel_sh_id, result)

    def cascade_delete(self, sh_id):
        """
        级联删除分享表(由于太危险，现在改为检测式删除了，此方法可用作内部的数据管理)
        :param sh_id:
        :return:
        """
        from common.tassadar.meta.relation import Relation
        from common.tassadar.meta.tb import TB
        relation_model = Relation()
        tb_model = TB()
        cols = (Share.TB_ID, Share.SHARER)
        cur_sh = self.get_one(sh_id, cols)
        if not cur_sh:
            self.logger.warn('要删除的分享表不存在：%s' % sh_id)
            return False
        tb_info = tb_model._get_one({TB.TB_ID: cur_sh[Share.TB_ID]}, (TB.OWNER, ))
        if not tb_info:
            self.logger.warn('分享表的基础表不存在，sh_id: %s' % sh_id)
            return False
        rel_sh_ids = [sh_id]
        self.get_rel_sh_tbs(sh_id, rel_sh_ids)

        for sh_id in rel_sh_ids:
            # 需要删除此表的所有依赖表
            rel_tb_infos = []
            relation_model.get_all_rel_tables(sh_id, rel_tb_infos)
            # 找出所有依赖此分享表的合表
            rel_tbs = []
            for tb_info in rel_tb_infos:
                rel_tbs.append(tb_info['tb_id'])
            for tb_id in rel_tbs:
                tb_model.cascade_delete(tb_id)
            self._delete({Share.SH_ID: sh_id})
        self.logger.warn('delete share: %s' % ','.join(rel_sh_ids))
        return True

    def delete_tb_share(self, tb_id):
        """
        删除某个表的所有分享记录
        :return:
        """
        sh_id_list = self.get({Share.TB_ID: tb_id}, (Share.SH_ID, ))
        for sh_id in sh_id_list:
            self.cascade_delete(sh_id)

    def get_share_chain(self, tb_id, user_id, groups = []):
        """
            获取一个表的分享链
            比如 tb_owner把表分给A，A分给B，B分给user_id
            则返回的结果是[tb_owner, A, B, user_id]

            还要考虑组的情况
        """
        if not groups:
            try:
                groups = OverlordClient().get_user_by_anonymous(user_id, 1)
                groups = groups.get('belong_group', [])
            except:
                pass
        dep_ids = []
        result = set([user_id])
        sh = self.get({Share.TB_ID: tb_id, Share.TO_USER: user_id}, cols=(Share.SH_ID, Share.SHARER, Share.DEP_ID))
        for s in sh:
            result.add(s[Share.SHARER])
            if s[Share.DEP_ID]:
                dep_ids.append(s[Share.DEP_ID])

        if len(groups):
            sh = self.get({Share.TB_ID: tb_id, Share.TO_GROUP: groups}, cols=(Share.SH_ID, Share.SHARER, Share.DEP_ID))
            for s in sh:
                result.add(s[Share.SHARER])
                if s[Share.DEP_ID]:
                    dep_ids.append(s[Share.DEP_ID])
        loop_count = 0
        while len(dep_ids) and loop_count < MAX_SHARE_COUNT:
            sh = self.get({Share.SH_ID: dep_ids}, cols=(Share.SH_ID, Share.SHARER, Share.DEP_ID))
            dep_ids = []
            for s in sh:
                result.add(s[Share.SHARER])
                if s[Share.DEP_ID]:
                    dep_ids.append(s[Share.DEP_ID])
            loop_count += 1

        return list(result)
