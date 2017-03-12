#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json

import common.tassadar.meta.conn
import common.database
import common.tools as Tools
from common.logger import Logger


class MetaBase(Logger):
    """数据表基础操作
    ctime通过_create方法自动添加，utime通过数据库自动更新，更新数据时无需单独设置utime
    添加或更新数据时，允许传入dict和list对象，自动转换为json字符串
    """
    _table = 'test'

    def __init__(self):
        super(MetaBase, self).__init__()
        self.db = common.tassadar.meta.conn.DBTassadar()

    def _formatter(self, pairs, delimer=" AND "):
        if not pairs:
            return '1 = 1'
        values = []
        for key, value in pairs.items():
            if isinstance(value, list):
                if value:
                    values.append("`%s` in ('%s')" % (key, "','".join([self.db.escape_string(v) for v in value])))
                else:
                    # 如果数组为空，我们认为此字段为null
                    values.append("`%s` is null" % key)
            else:
                value = self.db.escape_string(value)
                values.append("`%s`='%s'" % (key, value))
        return delimer.join(values)

    def _create(self, params):
        if 'ctime' not in params:
            params['ctime'] = Tools.current_time()
        for key in params:
            if isinstance(params[key], dict) or isinstance(params[key], list):
                params[key] = json.dumps(params[key])

        return self.db.insert_dict(self._table, params)

    def _update(self, conds, params):
        for key in params:
            if isinstance(params[key], dict) or isinstance(params[key], list):
                params[key] = json.dumps(params[key])
        return self.db.update_dict(self._table, params, conds)

    def _delete(self, conds):
        return self.db.update_dict(self._table, {'is_del': 1}, conds)

    def _delete_force(self, conds):
        sql = "DELETE FROM `%s` WHERE %s" % (self._table, self._formatter(conds))
        return self.execute(sql)


    def _get_one(self, conds, cols=(), filter_delete=True):
        col = "`%s`" % '`,`'.join(cols) if cols else '*'
        if filter_delete:
            conds['is_del'] = 0
        sql = "SELECT %s FROM %s WHERE %s" % (col, self._table, self._formatter(conds))
        result = self.db.query_one(sql)
        if not cols and 'is_del' in result:
            del(result['is_del'])
        return result

    def count(self, conds, filter_delete=True):
        if filter_delete:
            conds['is_del'] = 0
        sql = "SELECT count(1) as c FROM %s WHERE %s" % (
            self._table, self._formatter(conds))
        results = self.db.query(sql)
        return results[0]['c']

    # todo: limit
    def get(self, conds, cols, offset=0, limit=2500, filter_delete=True, order="ctime"):
        col = "`%s`" % '`,`'.join(cols) if cols else '*'
        if filter_delete:
            conds['is_del'] = 0
        sql = "SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %s, %s" % (
            col, self._table, self._formatter(conds), order, offset, limit)
        results = self.db.query(sql)
        if not cols:
            for result in results:
                if 'is_del' in result:
                    del(result['is_del'])
        return results

    def fetchall(self, sql):
        return self.db.query(sql)

    def execute(self, sql):
        return self.db.write(sql)
