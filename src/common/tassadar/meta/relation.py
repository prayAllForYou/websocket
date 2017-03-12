#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy
from metabase import MetaBase
from share import Share


class Relation(MetaBase):
    """
    表之间的依赖关系（合表的依赖表）
    """
    _table = 'RELATION'
    """字段定义"""
    TB_ID = 'tb_id'
    DEP_ID = 'dep_id'

    def create(self, tb_id, dep_id):
        return self._create({
            Relation.TB_ID: tb_id,
            Relation.DEP_ID: dep_id
        })

    def get_rel_tables(self, tb_id):
        """
        获取指定表的被依赖表
        :param tb_id:
        :return:
        """
        return self.get({Relation.DEP_ID: tb_id}, (Relation.TB_ID, ))

    def get_all_rel_tables(self, tb_id, result, dep=0, index=0):
        self._get_all_rel_tables(tb_id, result, {}, dep, index)

    def _get_all_rel_tables(self, tb_id, result, tb_map, dep=0, index=0):
        """
        获取一张表的所有被依赖表
        :param tb_id:
        :param result:
        :param dep:
        :param tb_map: 用于判断是否存在循环依赖，防止死循环
        :param index: 被依赖表的索引，用于按照依赖关系排序，保证合表次序不会错乱
        :return:
        """
        if tb_id in tb_map:
            raise Exception("there is a cycle in the relation graph")

        tb_map[tb_id] = dep
        share_model = Share()
        shares = share_model.get({Share.TB_ID: tb_id}, (Share.SH_ID, ))
        sh_ids = [s[Share.SH_ID] for s in shares]
        del share_model
        tb_ids = [tb_id] + sh_ids
        rel_tables = self.get_rel_tables(tb_ids)
        for rel_tb in rel_tables:
            rel_tb['id'] = index
            index += 1
        dep += 1
        for rel_table in rel_tables:
            rel_tb_id = rel_table[Relation.TB_ID]
            result.append(rel_table)
            index = self._get_all_rel_tables(rel_tb_id, result, tb_map, dep, index)
        tb_map.pop(tb_id)
        return index

    def get_all_dependencies(self, tb_id):
        share_model = Share()
        result = []
        self.get_dependencies(share_model, tb_id, result)
        distinct_result = []
        for r in result:
            added = False
            for dr in distinct_result:
                if dr['tb_id'] == r['tb_id']:
                    added = True
            if not added:
                distinct_result.append(r)
        return distinct_result

    def get_dependencies(self, share_model, origin_tb_ids, result):
        relation_data = self.get({Relation.TB_ID: origin_tb_ids}, (Relation.DEP_ID, ))
        if not relation_data:
            return None
        ids = [r[Relation.DEP_ID] for r in relation_data]
        tb_ids = [t_id for t_id in ids if t_id.find('sh_') != 0]
        sh_ids = [t_id for t_id in ids if t_id.find('sh_') == 0]
        for t in tb_ids:
            result.append({'tb_id': t, 'is_share': 0})
        if sh_ids:
            sh_list = share_model.get({Share.SH_ID: sh_ids}, (Share.TB_ID, ))
            sh_tb_ids = [s[Share.TB_ID] for s in sh_list]
            for s in sh_tb_ids:
                result.append({'tb_id': s, 'is_share': 1})
            tb_ids += sh_tb_ids
        self.get_dependencies(share_model, tb_ids, result)

    # TODO remove dup code later, for evidence
    # def get_cascade_update_plan(self, tb_id_list=[]):
    #     """
    #     此接口可以接收多个tb_id作为参数，根据级联更新关系给出一个执行级联更新的tb_id顺序
    #     :param tb_id_list:
    #     :return: 区别于get_all_rel_tables，此函数返回数列返回已经是排好序并去重后的结果
    #     """
    #     if not tb_id_list:
    #         return []
    #
    #     return_list = []
    #
    #     # 去除源tb_list对应的所有分配表
    #     share_model = Share()
    #     shares = share_model.get({Share.TB_ID: tb_id_list}, (Share.SH_ID, ))
    #     sh_ids = [s[Share.SH_ID] for s in shares]
    #     tb_id_list += sh_ids
    #
    #     # 纪录第一层的所有tb_id, 这些tb_id是不用加入执行计划的，因为他们的类型不是view
    #     origin_tb_id_list = copy.deepcopy(tb_id_list)
    #
    #     # 广度遍历并去重，如果有依赖重复出线的情况下，取更靠后的任务以保证任务依赖的正确
    #     while tb_id_list:
    #         tb_id = tb_id_list[0]
    #         # 第一层的tb是不用加入到结果的，因为不是gen类型的
    #         if tb_id in origin_tb_id_list:
    #             pass
    #         else:
    #             if tb_id not in return_list:
    #                 return_list.append(tb_id)
    #
    #         share_tb_list = [share[Share.SH_ID] for share in share_model.get({Share.TB_ID: tb_id}, [Share.SH_ID])]
    #         tb_ids = [tb_id] + share_tb_list
    #
    #         ref_tb_list = [tmp_tb[Relation.TB_ID] for tmp_tb in self.get_rel_tables(tb_ids)]
    #         for tmp_tb_id in ref_tb_list:
    #             if tmp_tb_id in return_list:
    #                 return_list.remove(tmp_tb_id)
    #
    #             tb_id_list.append(tmp_tb_id)
    #
    #         tb_id_list.pop(0)
    #
    #     return return_list

    def get_cascade_update_plan(self, tb_id_list=[]):
        """
        此接口可以接收多个tb_id作为参数，根据级联更新关系给出一个执行级联更新的tb_id顺序
        :param tb_id_list:
        :return: 区别于get_all_rel_tables，此函数返回数列返回已经是排好序并去重后的结果
        """
        if not tb_id_list:
            return []

        return_list = []

        # 去除源tb_list对应的所有分配表
        share_model = Share()
        shares = share_model.get({Share.TB_ID: tb_id_list}, (Share.SH_ID, ))
        sh_ids = [s[Share.SH_ID] for s in shares]
        tb_id_list += sh_ids

        # 纪录第一层的所有tb_id, 这些tb_id是不用加入执行计划的，因为他们的类型不是view
        origin_tb_id_list = copy.deepcopy(tb_id_list)

        # 广度遍历并去重，如果有依赖重复出线的情况下，取更靠后的任务以保证任务依赖的正确
        while tb_id_list:
            # 第一层的tb是不用加入到结果的，因为不是gen类型的
            for tb_id in tb_id_list:
                if tb_id in return_list:
                    return_list.remove(tb_id)

                return_list.append(tb_id)

            share_tb_list = [share[Share.SH_ID] for share in share_model.get({Share.TB_ID: tb_id_list}, [Share.SH_ID])]
            tb_ids = list(set(tb_id_list + share_tb_list))

            tb_id_list = list(set([tmp_tb[Relation.TB_ID] for tmp_tb in self.get_rel_tables(tb_ids)]))

        return_list = [r for r in return_list if r not in origin_tb_id_list]
        return return_list

    def delete_rel_info(self, tb_id):
        """
        删除此表的依赖关系
        :param tb_id:
        :return:
        """
        self._delete({Relation.TB_ID: tb_id})

    def delete(self, tb_id, dep_id):
        self._delete({Relation.TB_ID: tb_id, Relation.DEP_ID: dep_id})

    def del_rel_tables(self, tb_id):
        """
        递归删除所有相关依赖表(删除此表的依赖信息，和被依赖信息)
        暂时不用
        :param tb_id:
        :return:
        """
        self._delete({Relation.DEP_ID: tb_id})
        # 删除分享表
        Share()._delete({Share.TB_ID: tb_id})
        for rel_table in self.get_rel_tables(tb_id):
            tb_id = rel_table[Relation.TB_ID]
            self.del_rel_tables(tb_id)

    def cal_tb_gen_rely_count(self, tb_id_list=[], full_tb_id_list=[]):

        sql = """
            SELECT A.tb_id, coalesce(if(A.dep_id LIKE %%s, NULL, A.dep_id), B.tb_id) AS target_tb_id
            FROM RELATION A
            LEFT JOIN SHARED_TB B ON A.dep_id = B.sh_id
            AND B.is_del = 0
            WHERE A.tb_id IN ('%s') AND A.is_del = 0
        """ % """','""".join([str(v) for v in tb_id_list])

        res = self.db.query(sql, "sh_%")
        result = {}
        for tb_id in tb_id_list:
            execute_tb_list = [r['target_tb_id'] for r in res if tb_id == r['tb_id']]
            result[tb_id] = len(set(execute_tb_list).intersection(set(full_tb_id_list)))

        return result

    def cal_tb_gen_rely_info(self, tb_id_list=[], full_tb_id_list=[]):

        sql = """
            SELECT A.tb_id, coalesce(if(A.dep_id LIKE %%s, NULL, A.dep_id), B.tb_id) AS target_tb_id
            FROM RELATION A
            LEFT JOIN SHARED_TB B ON A.dep_id = B.sh_id
            AND B.is_del = 0
            WHERE A.tb_id IN ('%s') AND A.is_del = 0
        """ % """','""".join([str(v) for v in tb_id_list])

        res = self.db.query(sql, "sh_%")
        result = {}
        for tb_id in tb_id_list:
            execute_tb_list = [r['target_tb_id'] for r in res if tb_id == r['tb_id']]
            result[tb_id] = {
                'rely_count': len(set(execute_tb_list).intersection(set(full_tb_id_list))),
                'rely_tbs': list(set(execute_tb_list).intersection(set(full_tb_id_list)))
            }

        return result
