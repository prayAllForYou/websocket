#!/usr/bin/env python
# -*- coding: utf-8 -*-

from metabase import MetaBase


class FieldRelation(MetaBase):
    """
    字段之间的依赖关系
    """
    _table = 'FIELD_RELATION'

    """字段定义"""
    TB_ID        = 'tb_id'
    FIELD_ID     = 'field_id'
    DEP_TB_ID    = 'dep_tb_id'
    DEP_FIELD_ID = 'dep_field_id'
    TYPE         = 'type'
    IS_DEL       = 'is_del'

    def create(self, tb_id, field_id, dep_tb_id, dep_field_id, type = 0):
        return self._create({
            FieldRelation.TB_ID: tb_id,
            FieldRelation.FIELD_ID: field_id,
            FieldRelation.DEP_TB_ID: dep_tb_id,
            FieldRelation.DEP_FIELD_ID: dep_field_id,
            FieldRelation.TYPE: type,
        })

    def get_rel_fields(self, tb_id, field_id, type = 0):
        """ 获取指定字段的被依赖字段
            :param tb_id:
            :param field_id:
        """
        return self.get({FieldRelation.DEP_TB_ID: tb_id, FieldRelation.DEP_FIELD_ID: field_id, FieldRelation.TYPE: type}, (FieldRelation.TB_ID, FieldRelation.FIELD_ID))

    def get_all_rels(self, tb_id, field_id, type = 0):
        """ 递归获取字段的所有依赖字段
        """
        result = []
        self._get_rels(tb_id, field_id, type, result)
        distinct_result = []
        adds = set()
        for r in result:
            seed = "%s-%s" % (r[FieldRelation.TB_ID], r[FieldRelation.FIELD_ID])
            if seed not in adds:
                adds.add(seed)
                distinct_result.append(r)
        return distinct_result

    def _get_rels(self, tb_id, field_id, type = 0, result = []):
        deps = self.get_rel_fields(tb_id, field_id, type)
        if deps:
            result.extend(deps)
            for d in deps:
                self._get_rels(d[FieldRelation.TB_ID], d[FieldRelation.FIELD_ID], type, result)

    def get_dep_fields(self, tb_id, field_id, type = 0):
        """ 获取指定字段的依赖字段
            :param tb_id:
            :param field_id:
        """
        return self.get({FieldRelation.TB_ID: tb_id, FieldRelation.FIELD_ID: field_id, FieldRelation.TYPE: type}, (FieldRelation.DEP_TB_ID, FieldRelation.DEP_FIELD_ID))

    def get_all_deps(self, tb_id, field_id, type = 0):
        """ 递归获取字段的所有依赖字段
        """
        result = []
        self._get_deps(tb_id, field_id, type, result)
        distinct_result = []
        adds = set()
        for r in result:
            seed = "%s-%s" % (r[FieldRelation.DEP_TB_ID], r[FieldRelation.DEP_FIELD_ID])
            if seed not in adds:
                adds.add(seed)
                distinct_result.append(r)
        return distinct_result

    def _get_deps(self, tb_id, field_id, type = 0, result = []):
        deps = self.get_dep_fields(tb_id, field_id, type)
        if deps:
            result.extend(deps)
            for d in deps:
                self._get_deps(d[FieldRelation.DEP_TB_ID], d[FieldRelation.DEP_FIELD_ID], type, result)

    def delete_rel_info(self, tb_id, field_id):
        """ 删除此字段的所有依赖关系
        :param tb_id:
        :param field_id:
        :return:
        """
        self._delete({FieldRelation.TB_ID: tb_id, FieldRelation.FIELD_ID: field_id})

    def delete_dep_info(self, tb_id, field_id):
        """ 删除此字段的所有被依赖信息
        :param tb_id:
        :param field_id:
        :return:

        如果被删除的字段还有依赖，则要更新依赖关系
        """
        self._delete({FieldRelation.DEP_TB_ID: tb_id, FieldRelation.dep_field_id: field_id})

    def delete(self, tb_id, field_id, dep_tb_id, dep_field_id):
        self._delete({
            FieldRelation.TB_ID: tb_id,
            FieldRelation.FIELD_ID: field_id,
            FieldRelation.DEP_TB_ID: dep_tb_id,
            FieldRelation.DEP_FIELD_ID: dep_tb_id
        })
