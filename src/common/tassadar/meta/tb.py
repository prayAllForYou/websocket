#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from metabase import MetaBase
from gen import Gen
from relation import Relation
from field import Field, expand_vfield
from vfield import VField
from share import Share
import common.tools as tools
from common.mobius import Mobius
from common.redis_helper import RedisHelper

class TB(MetaBase):
    """
    虚拟数据表
    """
    _table = 'TB'
    '''字段类型定义'''
    TB_ID = 'tb_id'
    DB_ID = 'db_id'
    STORAGE_ID = 'storage_id'
    DATA_COUNT = 'data_count'
    OWNER = 'owner'
    NAME = 'name'
    TITLE = 'title'
    TYPE = 'type'
    PARTITION = 'partition'
    STATUS = 'status'
    VERSION = 'version'
    UPDATE_VERSION = 'update_version'
    IVERSION = 'iVersion'
    CTIME = 'ctime'
    UTIME = 'utime'
    DATA_UTIME = 'data_utime'
    ERROR_MSG = 'error_msg'
    DATA_SIZE = 'data_size'
    PATCH_SIZE = 'patch_size'
    CACHE_IN_MOBIUS = "cache_in_mobius"
    REPARTITION_SIZE = 'repartition_size'
    USAGE = 'usage'
    TAG = 'tag'
    LABEL = 'label'  #标签
    COMMENT = 'comment'  #备注
    # 额外字段，不做存储，从Field表中拼装出来
    FIELDS = 'fields'
    ROW_FILTER = 'row_filter'
    '''数据表类型定义'''
    EXCEL_TYPE = 0
    OPENDS_TYPE = 1
    DS_TYPE = 2
    VIEW_TYPE = 3
    SHARED_TYPE = 4
    PUBLIC_TYPE = 5
    BUSINESS_TYPE = 6   # 业务表,其他系统同步过来的,如SEM固化项目

    # 跨域合并类型
    GATHER_TYPE = 5
    '''数据表状态定义'''
    CREATE_STATUS = 0
    SYNC_FINISH = 1
    SYNC_ERROR = 2
    MERGE_ERROR = 2
    SYNCING_STATUS = 3
    MIGRATE = 4
    MIGRATE_ERROR = 5
    QUEUEN_UP = 6

    '''分区设置'''
    PARTITION_BASE_FIELD = 'base_field'
    PARTITION_FORMULA = 'formula'
    PARTITION_SET_VERSION = 'set_version'
    PARTITION_PARAM = 'param'

    '''commit频次限制'''
    COMMIT_LIMIT = 'commit_limit'

    def create(self, params):
        # 检查必填字段
        required_params = (TB.OWNER, TB.NAME, TB.TYPE, TB.FIELDS)
        for rp in required_params:
            if rp not in params:
                self.logger.warn('缺失字段：%s' % rp)
                return None

        if TB.TB_ID not in params:
            params[TB.TB_ID] = tools.uniq_id('tb')
        # 表字段处理
        for field in params[TB.FIELDS]:
            field[Field.TB_ID] = params[TB.TB_ID]
            Field().create(field)
        del (params[TB.FIELDS])

        if self._create(params):
            return params[TB.TB_ID]
        else:
            return None

    # todo: test
    def find_parent_sh(self, sh_links, sh_id, result):
        for sh in sh_links:
            if sh[Share.SH_ID] == sh_id:
                result.append(sh)
                if sh[Share.DEP_ID]:
                    self.find_parent_sh(sh_links, sh[Share.DEP_ID], result)
                break

    # 加入user_id根据当前user_id和groups，附加其特有的计算字段
    def get_tb(self, tb_id, cols=(), user_id=None, groups = []):
        """
        获取表、分配表的基本信息，返回字段列表等信息，默认返回表的所有信息
        如果需要获取表的使用者创建的计算字段信息，必须传user_id
        :param tb_id:
        :param cols:
        :param user_id:
        :return:
        """
        # 如果是sh_开头的tb_id一定是分享表id，分享表在使用上和普通的表是一样的，只是多出了过滤条件信息
        # todo 需要对删除虚拟列的Aggregator
        if tb_id.find('sh_') == 0:
            sh_id = tb_id
            share_model = Share()
            share_info = share_model.get_one(sh_id)
            if not share_info:
                self.logger.warn('分享表不存在：' + sh_id)
                return None

            tb_id = share_info[Share.TB_ID]
            tb_info = self.get_tb_info(tb_id, user_id, cols, groups=groups)
            if not tb_info:
                self.logger.warn('分享表对应的数据表不存在：sh_id:%s, tb_id:%s' % (sh_id, tb_id))
                return None
            tb_info[Share.ROW_FILTER] = share_info[Share.ROW_FILTER]
            tb_info[Share.IS_FIXED] = share_info[Share.IS_FIXED]
            # 使用share的version替换tb的version，因为share的过滤条件有可能有修改，需要单独记录版本号
            tb_info[TB.VERSION] = share_info[Share.VERSION]
            tb_info[TB.DATA_COUNT] = share_info[Share.DATA_COUNT]
            return tb_info
        else:
            tb_info = self.get_tb_info(tb_id, user_id, cols)
            if not tb_info:
                return None
            # fields = tb_info[TB.FIELDS]
            # self.change_aggregator(fields)
            return tb_info

    def change_aggregator(self, fields):
        # 如果是虚拟列  就将Aggregator 删除
        # 用戶在使用感受上和普通字段無區別
        for f in fields:
            if VField.AGGREGATOR in f and VField.FLAG in f and f[VField.FLAG] == VField.TABLE_VFIELD_FLAG:
                f[VField.ROW_AGGREGATOR] = f[VField.AGGREGATOR]
                del f[VField.AGGREGATOR]

    def get_tb_info(self, tb_id, user_id, cols=(), filter_delete=True, groups=[]):
        """
        获取表信息，包括表的字段信息
        字段包含所有类型的字段，通过包含'flag'key的计算字段
        :param conds:
        :param cols:
        :param user_id: 附加的user_id，用于附加额外的计算字段（使用分配表的用户创建的计算字段）
        :param filter_delete:
        :return:
        """
        fields = list(cols)
        append_fields = [TB.TB_ID, TB.OWNER, TB.VERSION]
        if fields:
            for f in append_fields:
                if f not in fields:
                    fields.append(f)
        tb_info = self._get_one({TB.TB_ID: tb_id}, fields, filter_delete)
        if not tb_info:
            return None
        field_conds = (Field.FIELD_ID, Field.NAME, Field.TITLE, Field.TYPE, Field.SEQ_NO, Field.UNIQ_INDEX, Field.REMARK)
        real_fields = Field().get_list(tb_info[TB.TB_ID], field_conds)
        vfield_conds = (
            VField.FIELD_ID, VField.NAME, VField.TITLE, VField.TYPE, VField.AGGREGATOR, VField.OWNER,
            VField.SEQ_NO, VField.PARAM, VField.FLAG, VField.TB_ID, VField.CTIME, VField.REMARK)
        # 计算字段如果制定了特定的用户，需要将指定用户的计算字段也加上
        # update BDP-3378 多次分享后最后一个用户能看到前面所有用户创建的计算字段
        user_info = tb_info[TB.OWNER]
        if user_id and user_id != tb_info[TB.OWNER]:
            user_info = Share().get_share_chain(tb_id, user_id, groups)
            # user_info = [tb_info[TB.OWNER], user_id]
        virtual_fields = VField().get_list(tb_info[TB.TB_ID], user_info, vfield_conds)
        for vf in virtual_fields:
            vf[VField.RAW_FORMULA] = expand_vfield(vf[VField.AGGREGATOR], virtual_fields)
        # 加入计算字段
        tb_info[TB.FIELDS] = real_fields + virtual_fields
        return tb_info

    # todo: 分享表，简化字段
    def get_tb_infos(self, tb_ids, cols=()):
        fields = list(cols)
        append_fields = [TB.TB_ID, TB.OWNER]
        if fields:
            for f in append_fields:
                if f not in fields:
                    fields.append(f)
        tb_info = self.get({TB.TB_ID: tb_ids}, fields)
        if not tb_info:
            return None
        field_conds = (Field.TB_ID, Field.FIELD_ID, Field.NAME, Field.TITLE, Field.TYPE, Field.SEQ_NO, Field.UNIQ_INDEX)
        real_fields = Field().get_list(tb_ids, field_conds, limit=10000)
        vfield_conds = (
            VField.TB_ID, VField.FIELD_ID, VField.NAME, VField.TITLE, VField.TYPE, VField.AGGREGATOR, VField.SEQ_NO,
            VField.PARAM, VField.FLAG)
        virtual_fields = VField().get_list(tb_ids, tb_info[0][TB.OWNER], vfield_conds, limit = 10000)
        for r in tb_info:
            r_fields = [rf for rf in real_fields if rf[Field.TB_ID] == r[TB.TB_ID]]
            v_fields = [vf for vf in virtual_fields if vf[VField.TB_ID] == r[TB.TB_ID]]
            # 加入计算字段
            r[TB.FIELDS] = r_fields + v_fields
        return tb_info

    # todo: 简化查询参数
    def get_one(self, conds, cols=(), filter_delete=True):
        fields = list(cols)
        append_fields = [TB.TB_ID, TB.OWNER]
        if fields:
            for f in append_fields:
                if f not in fields:
                    fields.append(f)
        res = self._get_one(conds, fields, filter_delete)
        if not res:
            return None
        return res

    def get_tb_schema(self, conds, cols=(), filter_delete=True):
        """
        只包含tb的基础字段信息
        :param conds:
        :param cols:
        :param filter_delete:
        :return:
        """
        fields = list(cols)
        append_fields = [TB.TB_ID, TB.OWNER]
        if fields:
            for f in append_fields:
                if f not in fields:
                    fields.append(f)
        res = self._get_one(conds, fields, filter_delete)
        if not res:
            return None
        real_fields = Field().get_list(res[TB.TB_ID], (Field.FIELD_ID, Field.TYPE, Field.NAME, Field.TITLE))
        res[TB.FIELDS] = real_fields
        return res

    def get_list(self, owner, cols=(), offset=0, limit=1000):
        return self.get({TB.OWNER: owner}, cols, offset, limit)

    def update(self, tb_id, params, update_share_version=True, is_del=False):
        if TB.VERSION in params and update_share_version:
            # 更新此表依赖的分享表的version
            share_model = Share()
            shares = share_model.get({Share.TB_ID: tb_id}, (Share.SH_ID, Share.VERSION, Share.ROW_FILTER, Share.DEP_ID))
            if shares:
                cond = (VField.FIELD_ID, VField.TITLE, VField.AGGREGATOR)
                table_vfield = VField().get({VField.TB_ID: tb_id, VField.FLAG: VField.TABLE_VFIELD_FLAG}, cond)
                tb_info = self.get_one({TB.TB_ID: tb_id}, (TB.STORAGE_ID, ))
                storage_id = tb_info[TB.STORAGE_ID]
                # todo: 这里依赖mobius实现的不太好
                mobius = Mobius()
                for share in shares:
                    share[Share.VERSION] = 0 if share[Share.VERSION] is None else share[Share.VERSION]
                    update_info = {Share.VERSION: share[Share.VERSION] + 1}
                    # 更新分享表的数据条数
                    if share[Share.ROW_FILTER] == '[]' and not share[Share.DEP_ID]:
                        if TB.DATA_COUNT in params:
                            update_info[Share.DATA_COUNT] = params[TB.DATA_COUNT]
                    else:
                        if TB.DATA_COUNT in params and params[TB.DATA_COUNT] == 0:
                            update_info[Share.DATA_COUNT] = 0
                        else:
                            dep_list = [share]
                            self.find_parent_sh(shares, share[Share.SH_ID], dep_list)
                            row_filter = []
                            for sh in dep_list:
                                row_filter += json.loads(sh[Share.ROW_FILTER])
                            expand_filter = expand_vfield(row_filter, table_vfield)
                            where = ' and '.join(expand_filter)
                            if not expand_filter and TB.DATA_COUNT in params:
                                update_info[Share.DATA_COUNT] = params[TB.DATA_COUNT]
                            else:
                                try:
                                    count_data = mobius.query('select count(1) as count from %s where %s' % (storage_id, where))
                                    update_info[Share.DATA_COUNT] = count_data['data'][0][0]
                                except Exception as e:
                                    self.logger.error(e)
                    share_model.update(share[Share.SH_ID], update_info)
        return self._update({TB.TB_ID: tb_id, 'is_del': is_del}, params)

    def delete(self, tb_id):
        return self._delete({TB.TB_ID: tb_id})

    def cascade_delete(self, tb_id):
        relation_model = Relation()
        share_model = Share()
        gen_model = Gen()

        rel_tbs = [tb_id]
        # 需要删除此表的所有依赖表
        rel_tb_infos = []
        relation_model.get_all_rel_tables(tb_id, rel_tb_infos)
        for tb_info in rel_tb_infos:
            rel_tbs.append(tb_info['tb_id'])
        for tb_id in rel_tbs:
            # 删除数据表
            self.delete(tb_id)
            # 删除合表规则
            gen_model._delete({Gen.TB_ID: tb_id})
            # 删除分享表
            share_model.delete_tb_share(tb_id)
            # 删除此表的依赖关系
            relation_model.delete_rel_info(tb_id)
        self.logger.warn('delete tb: %s' % ','.join(rel_tbs))

    def stat_tb_size(self, user_id_list):
        redis = RedisHelper()
        tb_size = 0
        for user_id in user_id_list:
            # tassadar:tb/stat:user_id
            cache_key = 't:ts:%s' % user_id
            cache = redis.get_json(cache_key, expire=259200)
            if not cache:
                sql = "select sum(%s) as data_size from %s where owner='%s' and is_del=0 and type!=3" \
                      % (TB.DATA_SIZE, self._table, user_id)
                result = self.db.query_one(sql)
                if result["data_size"] is None:
                    result["data_size"] = 0
                cache_info = {'data_size' : long(result["data_size"])}
                redis.set_json(cache_key, cache_info, expire=259200)
                tb_size = tb_size + result["data_size"]
            else:
                tb_size = tb_size + cache["data_size"]
        return tb_size

