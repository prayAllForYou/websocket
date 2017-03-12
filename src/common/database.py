#!/usr/bin/python
# -*- coding: utf-8 -*-


import MySQLdb
from datetime import datetime
from common.torndb import Connection


class MySQLHelper:
    def __init__(self, host, user, password, database=None, pool_size=10, max_overflow=25):
        self._db = Connection(host, database, user=user, password=password, max_idle_time=10,
                              pool_size=pool_size, max_overflow=max_overflow)
        if not self._db._db:
            raise Exception('%s' % self._db.error_msg)

    def __format_record(self, record):
        for key in record.keys():
            if isinstance(record[key], datetime):
                record[key] = record[key].strftime('%Y-%m-%d %H:%M:%S')
        return record

    def __format_records(self, records):
        if isinstance(records, list):
            return map(self.__format_record, records)
        return self.__format_record(records)

    def query(self, sql, *parameters):
        return self.__format_records(self._db.query(sql, *parameters))

    def query_one(self, sql, *parameters):
        res = self._db.query(sql, *parameters)
        return self.__format_record(res.pop()) if res else {}

    def write(self, sql, *parameters):
        return self._db.execute(sql, *parameters)

    def gen_insert(self, tablename, rowdict, replace=False):
        return self._db.gen_insert_sql(tablename, rowdict, replace)

    def insert_dict(self, tablename, rowdict, replace=False):
        return self._db.insert_dict(tablename, rowdict, replace)

    def update_dict(self, tablename, rowdict, where):
        return self._db.update_dict(tablename, rowdict, where)

    def transaction(self, query, parameters):
        return self._db.transaction(query, parameters)

    def escape_string(self, string):
        if not string:
            return str(string)
        if isinstance(string, (str, basestring)):
            return MySQLdb.escape_string(string)
        return string

    def __del__(self):
        self._db.close()
