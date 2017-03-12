#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Mobius api请求辅助类

接口定义：http://git.haizhi.com/bdp-server/mobius/tree/master
"""
import json
import os
import time
import requests
from requests.exceptions import ConnectionError, ReadTimeout
import common.tools
import common.config
import threading
from logging import getLogger
thread_local = threading.local()


class MobiusException(Exception):
    pass

class MobiusVerboseException(Exception):
    def __init__(self, errstr, status):
        self.errstr = errstr
        self.status = status

    def __str__(self):
        return self.errstr

@common.tools.singleton
class Mobius():
    """
    Mobius交互模块
    """
    _lock = threading.RLock()

    def __init__(self):
        self.spec_urls = {}
        self._load_config()
        self.query_active = 0

    def _load_config(self):
        self._get_url_prefix()
        # 在conf里读取query限制数量
        self.query_limit = int(common.config.get('global', 'mobius_query_limit'))
        spec_urls_path = os.path.join(os.getcwd(), 'conf/mobius_spec_urls.json')
        if os.path.exists(spec_urls_path):
            with open(spec_urls_path, 'r') as f:
                self.spec_urls = json.loads(f.read())

    # 重载mobius客户端需要重写_get_url_prefix这个函数，来制定mobius的访问路径
    def _get_url_prefix(self):
        self.url_prefix = common.config.get('service', 'mobius').split(',')
        self.url_prefix_deploy = common.config.get('service', 'mobius_deploy').split(',')

    def _open(self, short_uri):
        res = None
        is_deploy = False
        if os.path.exists('/tmp/deploy'):
            base_urls = self.url_prefix_deploy
            is_deploy = True
        else:
            base_urls = self.url_prefix + self.url_prefix_deploy

        try_count = 0
        base_url_len = len(base_urls)

        ts = time.time()

        while try_count < 30:
            base_url = base_urls[try_count % base_url_len]
            short_uri_without_params = short_uri.split('?')[0]
            prefix_url = base_url if is_deploy else self.spec_urls.get(short_uri_without_params, base_url)
            url_normal = os.path.join(prefix_url, short_uri)
            try:
                getLogger("mobius_performance").debug("requesting [%s]", url_normal)
                print url_normal
                res = requests.get(url_normal)
                if res.status_code == 502:
                    raise ConnectionError('status code %s' % res.status_code)
                break
            except ReadTimeout:
                raise MobiusException('mobius task timeout while processing ' + short_uri)
            except ConnectionError as e:
                try_count += 1
                time.sleep(2)
                print 'can not connect to mobius, retry ...'
                if try_count == 30:
                    raise e

        te = time.time()

        getLogger("mobius_performance").info("[%s] [%f] ", short_uri, te - ts)
        return res

    def _request(self, short_uri, payload, timeout=3000):
        """
        核心http请求
        :param short_uri:
        :param payload:
        :param timeout: 请求超时时间(s)
        :return:
        """
        res = None
        is_deploy = False
        if os.path.exists('/tmp/deploy'):
            base_urls = self.url_prefix_deploy
            is_deploy = True
        else:
            base_urls = self.url_prefix + self.url_prefix_deploy

        payload['trace_id'] = getattr(thread_local, "trace_id", "")

        try_count = 0
        base_url_len = len(base_urls)

        ts = time.time()

        while try_count < 30:
            base_url = base_urls[try_count % base_url_len]
            short_uri_without_params = short_uri.split('?')[0]
            prefix_url = base_url if is_deploy else self.spec_urls.get(short_uri_without_params, base_url)
            url_normal = os.path.join(prefix_url, short_uri)

            try:
                getLogger("mobius_performance").debug("requesting [%s]...[%s]", url_normal, payload["trace_id"])
                res = requests.post(url_normal, data=payload, timeout=timeout)
                if res.status_code == 502:
                    raise ConnectionError('status code %s' % res.status_code)
                break
            except ReadTimeout:
                raise MobiusException('mobius task timeout while processing ' + short_uri)
            except ConnectionError as e:
                try_count += 1
                time.sleep(2)
                print 'can not connect to mobius, retry ...'
                if try_count == 30:
                    raise e

        te = time.time()

        getLogger("mobius_performance").info("[%s] [%f] [%s]", short_uri, te - ts, payload["trace_id"])

        try:
            result = res.json()
        except:
            raise MobiusException('mobius data format error: %s' % res.text)
        if result['status'] != 0:
            if result['status'] == 21:
                raise MobiusVerboseException('from mobius -> %s' % result['msg'], 21)
            else:
                raise MobiusException('from mobius -> %s' % result['msg'])
        return result

    def _write_data(self, user_id, storage_id, action, fields, data):
        payload = dict(
            userId=user_id,
            tbName=storage_id,
            action=action,
            fields=fields,
            data=data
        )
        self._request(Urls.tb_write, payload)

    def write_mix_data(self, user_id, storage_id, fields, data):
        payload = dict(
            userId=user_id,
            tbName=storage_id,
            fields=fields,
            mixData=data
        )
        self._request(Urls.tb_write_mix_data, payload)

    def combine_data(self, source, target, source_fields, target_fields):
        payload = dict(
            source=source,
            target=target,
            sourceFields=source_fields,
            targetFields=target_fields
        )
        self._request(Urls.tb_combine, payload)

    # 控制query并发数量的函数入口
    def query_entry(self, payload, short_url):
        can_query = True
        # 连接数 + 1
        self._lock.acquire()
        self.query_active += 1
        # 判断是否超过限制
        if 0 <= self.query_limit < self.query_active:
            can_query = False
        self._lock.release()
        try:
            if can_query:
                req = self._request(short_url, payload)
        except Exception as e:
            raise e
        finally:
            # 连接数 - 1
            self._lock.acquire()
            self.query_active -= 1
            self._lock.release()
        if not can_query:
            raise MobiusException('server is busy, please try again later.')
        # 返回结果
        return req

    def query(self, sql, trace_id=None, group=0, storage_id=None):
        """
        执行原生的sql查询
        :param sql:
        :param trace_id:
        :param group:
        :param storage_id:
        :return:{'data':[], 'schema': []}
        """
        payload = dict(sql=sql, trace_id=trace_id, storage_id=storage_id)
        short_url = '%s?group=%s' % (Urls.db_query, group)
        return self.query_entry(payload, short_url)

    def view_preview(self, sql, trace_id=None):
        """
        执行原生的sql查询for view Preview
        :param sql:
        :param trace_id:
        :return:{'data':[], 'schema': []}
        """
        payload = dict(sql=sql, trace_id=trace_id)
        return self._request(Urls.db_viewPreview, payload)

    def adv_query(self, sql, temp_tables, trace_id=None, group=0, storage_id=None):
        payload = dict(sql=sql, temp_tables=temp_tables, trace_id=trace_id, storage_id=storage_id)
        short_url = '%s?group=%s' % (Urls.db_adv_query, group)
        return self.query_entry(payload, short_url)

    def adv_query2(self, query_tables, temp_tables, trace_id=None, group=0, storage_id=None):
        payload = dict(query_tables=query_tables, temp_tables=temp_tables, trace_id=trace_id, storage_id=storage_id)
        short_url = '%s?group=%s' % (Urls.db_adv_query2, group)
        return self.query_entry(payload, short_url)

    def export(self, query_tables, temp_tables, user_id, trace_id, group, version):
        payload = dict(query_tables=query_tables, temp_tables=temp_tables, user_id=user_id,
                       trace_id=trace_id, version=version)
        short_url = '%s?group=%s' % (Urls.query_export, group)
        return self._request(short_url, payload)

    def open_stream(self, export_id, accept_language='zh'):
        short_uri = Urls.open_stream+"?exportId=" + export_id + "&acceptLanguage=" + accept_language
        return self._open(short_uri)

    def ml_train(self, query_table, temp_tables, train_option, model_type, dimension_count, trace_id=None):
        payload = dict(query_table=query_table, temp_tables=json.dumps(temp_tables), trace_id=trace_id, train_option=json.dumps(train_option), model_type=model_type, dimension_count=dimension_count)
        return self._request(Urls.ml_train, payload)

    def ml_predict(self, query_table, temp_tables, model_params, model_type, dimension_count, trace_id=None):
        payload = dict(query_table=query_table, temp_tables=json.dumps(temp_tables), trace_id=trace_id, model_params=json.dumps(model_params), model_type=model_type, dimension_count=dimension_count)
        return self._request(Urls.ml_predict, payload)

    def hive_query(self, sql, trace_id=None):
        """
        执行hql，仅当spark执行合表失败时才会用到这个方法，让hive去合表
        :param sql:
        :param trace_id:
        :return:
        """
        payload = dict(sql=sql, trace_id=trace_id)
        return self._request(Urls.db_hive, payload)

    def create_table(self, user_id, schema, tbName=None):
        """
        创建数据表
        :param user_id:
        :param schema:json字符串数组
        :param tbName:
        :return:uuid的表名，表名为空表示创建失败
        """
        payload = dict(
            userId=user_id,
            schema=json.dumps(schema),
            tbName=tbName
        )
        r = self._request(Urls.tb_create, payload)
        return r['tbName']

    def get_table_info(self, storage_id):
        payload = dict(
            tbName=storage_id
        )
        return self._request(Urls.tb_info, payload)

    def modify_table(self, storage_id, schema):
        payload = dict(
            tbName=storage_id,
            schema=json.dumps(schema)
        )
        return self._request(Urls.tb_modify, payload)

    def migrate_table(self, user_id, tb_name, sql):
        payload = dict(
            userId=user_id,
            tbName=tb_name,
            sql=sql
        )
        return self._request(Urls.tb_migrate, payload)

    def insert_data(self, user_id, storage_id, fields, data):
        self._write_data(user_id, storage_id, "insert", fields, data)

    def update_data(self, user_id, storage_id, fields, data):
        self._write_data(user_id, storage_id, "update", fields, data)

    def delete_data(self, user_id, storage_id, fields, data):
        self._write_data(user_id, storage_id, "delete", fields, data)

    def merge_table(self, user_id, storage_id, uniq_keys, is_fast=False):
        """
        合并表数据，在执行insert，update或delete操作后，需要执行merge操作将数据写入hive表
        :param user_id:
        :param storage_id:
        :param uniq_keys:主键列表,多个主键使用逗号隔开,为空表示使用所有列作为主键
        :param is_fast:是否执行快速merge，仅保证本批次写的数据去重，不保证全局去重，对于merge非常大的文件比较有利
        :return:
        """
        payload = dict(
            userId=user_id,
            tbName=storage_id,
            keys=uniq_keys
        )
        if is_fast:
            payload['fast'] = True
        result = self._request(Urls.tb_merge, payload)
        return result['result']

    def merge_file_to_table(self, user_id, storage_id, fields, separator, null_holder):
        """
        对集群的文件进行merge处理
        :param user_id:
        :param storage_id:
        :param fields: 逗号分隔的字符串，和文件中的字段顺序要一致
        :param separator:
        :param null_holder:
        :return:
        """
        payload = dict(
            userId=user_id,
            tbName=storage_id,
            fields=fields,
            separator=separator,
            nullHolder=null_holder,
        )
        result = self._request(Urls.tb_merge_file, payload)
        return result['result']

    def clean_data(self, user_id, storage_id):
        """
        清空指定表的所有数据
        :param user_id:
        :param storage_id:
        :return:
        """
        payload = dict(
            userId=user_id,
            tbName=storage_id
        )
        self._request(Urls.tb_clean, payload)

    def tb_length(self, tb_path):
        """
        获取表的文件大小
        :param tb_path:
        :return:
        """
        payload = dict(
            file=tb_path
        )
        result = self._request(Urls.tb_length, payload)
        return result['result']

    def cal_tb(self, fields, data, formula):
        """
        使用spark的sql执行引擎对数据进行计算
        :param fields:
        :param data:
        :param formular:
        :return:
        """
        payload = dict(
            fields=json.dumps(fields),
            data=data,
            formula=json.dumps(formula)
        )
        return self._request(Urls.tb_cal, payload)

    def partition_tb(self, user_id, storage_id, formula, partition_key='pk'):
        """
        为工作表设置分区
        :param user_id:
        :param storage_id:
        :param formula:
        :param partition_key:
        :return:
        """
        payload = dict(
            userId=user_id,
            tbName=storage_id,
            formula=formula,
            pk=partition_key
        )
        return self._request(Urls.tb_partition, payload)

    def remove_tb_partition(self, user_id, storage_id, partition_key='pk'):
        """
        删除分区
        :param user_id:
        :param storage_id:
        :param partition_key:
        :return:
        """
        payload = dict(
            userId=user_id,
            tbName=storage_id,
            pk=partition_key
        )
        return self._request(Urls.tb_removePartition, payload)

    def get_table_status(self, storage_id):
        """
        获取指定表的状态
        :param storage_id:
        :return:0，正常，1，数据有变更，需要merge
        """
        payload = dict(tbName=storage_id)
        r = self._request(Urls.tb_status, payload)
        return r['tbStatus']

    def get_table_version(self, storage_id):
        """
        获取指定表的最新版本号
        :param storage_id:
        :return:版本号
        """
        payload = dict(tbName=storage_id)
        res = self._request(Urls.tb_status, payload)
        return res['version']

    def delete_table(self, user_id, storage_id):
        """
        {{{(>_<)}}}危险！
        物理删除数据表，会删除相关数据表的所有数据
        :param user_id:
        :param storage_id:
        :return:
        """
        payload = dict(
            userId=user_id,
            tbName=storage_id
        )
        return self._request(Urls.tb_delete, payload)

    def bulk_delete_data(self, user_id, storage_id, keys, where):
        """
        批量删除数据
        :param user_id:
        :param storage_id:
        :param keys:
        :param where:
        :return:
        """
        payload = dict(
            userId=user_id,
            tbName=storage_id,
            keys=keys,
            where=where
        )
        result = self._request(Urls.tb_bulkdelete, payload)
        return result['count']

    def migrate_view(self, tb_name, sql):
        payload = dict(
            tbName=tb_name,
            sql=sql
        )
        return self._request(Urls.view_migrate, payload)

    def create_view(self, tb_name, sql, map_join_enabled, partitions, engine, view_pool='default', base_temp_table=[]):
        payload = dict(
            tbName=tb_name,
            sql=sql,
            mapJoinEnabled=map_join_enabled,
            partitions=partitions,
            pool=view_pool,
            base_temp_table=json.dumps(base_temp_table)
        )
        short_url = '%s?engine=%s' % (Urls.view_create, engine)
        return self._request(short_url, payload)

    def create_union_table(self, storage_id, fields, dep_tbs):
        payload = dict(
            tbName=storage_id,
            fields=json.dumps(fields),
            depTbInfos=json.dumps(dep_tbs)
        )
        return self._request(Urls.tb_createUnion, payload)

    def create_real_view(self, tb_name, sql):
        payload = dict(
            tbName=tb_name,
            sql=sql
        )
        return self._request(Urls.view_real_create, payload)

    def generate_view(self, mapping, output, tb_name, engine, base_temp_table=[]):
        payload = dict(
            tbName=tb_name,
            mapping=mapping,
            output=output,
            base_temp_table=json.dumps(base_temp_table)
        )
        short_url = '%s?engine=%s' % (Urls.view_generate, engine)
        return self._request(short_url, payload)

    def generate_preview(self, mapping, output, limit=50):
        payload = dict(
            limit=limit,
            mapping=mapping,
            output=output
        )
        return self._request(Urls.preview_generate, payload)

    def cancel_job(self, gen_id):
        payload = dict(
            jobGroup=gen_id
        )
        return self._request(Urls.job_cancel, payload)

    def cache_table(self, storage_id):
        """
        把某个表在mobius中缓存
        :param storage_id:
        :return:
        """
        payload = dict(
            table=storage_id
        )
        return self._request(Urls.db_cache, payload)

    def uncache_table(self, storage_id):
        """
        取消某个表在mobius中的缓存
        :param storage_id:
        :return:
        """
        payload = dict(
            table=storage_id
        )
        return self._request(Urls.db_uncache, payload)

    def mobius_cal(self, cal_type, payload):
        """
        调用mobius进行高级计算
        :param cal_type 计算类型
        :param payload 计算时所需参数
        :return:
        """
        if cal_type == 'sankey':
            uri = Urls.cal_sankey

        if cal_type == "retention":
            uri = Urls.cal_retention
        return self._request(uri, payload)

    def check_data_exists(self, storage_id, fields, data):
        """
        调用mobius检查指定数据是否存在
        :param storage_id数据表的storage_id
        :param fields 包含列的列表, 格式为"fiedl1,field2,field3"
        :param data 数据value, 格式为[["a", "b", "c"],[...],[...]]
        :return:
        """
        payload = dict(
            tbName=storage_id,
            fields=fields,
            data=data
        )
        return self._request(Urls.check_data_exists, payload)

    def rename_table(self, old, new, is_table=True):
        if not is_table:
            return self.rename_view(old, new)
        sql = 'ALTER TABLE %s RENAME TO %s' % (old, new)
        try:
            self.query('drop table if exists %s' % new)
            self.query(sql)
        except Exception, e:
            print("rename table failed, sql: %s, %s" % (sql, e.message))
            return False
        return True

    def rename_view(self, old, new):
        sql = 'ALTER VIEW %s RENAME TO %s' % (old, new)
        try:
            self.query('DROP VIEW IF EXISTS %s' % new)
            self.query(sql)
        except Exception, e:
            print("rename view failed, sql: %s, %s" % (sql, e.message))
            return False
        return True

# mobius api set
Urls = common.tools.enum(
    db_query='db/query',
    db_viewPreview='db/viewPreview',
    db_adv_query='db/adv_query',
    db_adv_query2='db/adv_query2',
    query_export='query/export',
    db_hive='db/hive',
    tb_create='tb/create',
    tb_createUnion='tb/createUnionTable',
    tb_info='tb/info',
    tb_modify='tb/modify',
    tb_migrate='tb/migrate',
    tb_write='tb/write',
    tb_write_mix_data='tb/write/mixData',
    tb_combine='tb/combine',
    tb_bulkdelete='tb/bulkDelete',
    tb_delete='tb/delete',
    tb_merge='tb/merge',
    tb_merge_file='tb/merge/file',
    tb_clean='tb/clean',
    tb_version='tb/version',
    tb_revrt='tb/revert',
    tb_length='tb/length',
    tb_cal='tb/cal',
    tb_partition='tb/partition',
    tb_removePartition='tb/removePartition',
    view_migrate='view/migrate',
    view_create='view/create',
    view_real_create='view/createView',
    view_generate='view/generate',
    preview_generate='view/generatePreview',
    job_cancel='job/cancel',
    db_cache='db/cache',
    db_uncache='db/uncache',
    cal_sankey='adv_calc/sankey',
    cal_retention="adv_calc/retention",
    check_data_exists='tb/checkDataExists',
    ml_train='ml/train',
    ml_predict='ml/predict',
    open_stream='stream/download'
)

if __name__ == '__main__':
    mobius = Mobius()
    print mobius.query("select sex, count(distinct(username)) from contact group by sex")
