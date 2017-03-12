#!/usr/bin/env python
# encoding:utf-8


import json
import os
import common.tools
from common.base_client import BaseClient


class TassadarException(Exception):
    def __init__(self, msg, status=1):
        self.msg = msg
        self.status = status

    def __str__(self):
        return '<TassadarException: status %s, error %s>' % (self.status, self.msg)


class TassadarClient(BaseClient):
    """
    Tassadar交互模块
    """
    NORMAL_STATUS = '0'

    MODULE_NAME = "tassadar"
    CONF_SECTION = "tassadar"
    KEY_NAME = "url"

    def __init__(self):

        BaseClient.__init__(self)

        self.spec_urls = {}

    def init_config(self):

        BaseClient.init_config(self)

        spec_urls_path = os.path.join(os.getcwd(), 'conf/tassadar_spec_urls.json')
        if os.path.exists(spec_urls_path):
            with open(spec_urls_path, 'r') as f:
                self.spec_urls = json.load(f.read())

    def _request(self, short_uri, payload):

        res = self._do_request(short_uri, payload)
        result = res.json()

        if result['status'] != self.NORMAL_STATUS:
            raise TassadarException(result['errstr'], int(result['status']))
        return result['result']

    def create_db(self, user_id, type, name, title):
        payload = dict(
            user_id=user_id,
            type=type,
            name=name,
            title=title
        )
        return self._request(Urls.db_create, payload)

    def info_db(self, db_id):
        payload = dict(
            db_id=db_id
        )
        return self._request(Urls.db_info, payload)

    def list_db(self, user_id):
        payload = dict(
            user_id=user_id
        )
        return self._request(Urls.db_list, payload)

    def modify_db(self, db_id, title=None, status=None):
        payload = dict(
            db_id=db_id,
            title=title,
            status=status
        )
        return self._request(Urls.db_modify, payload)

    def delete_db(self, db_id):
        payload = dict(
            db_id=db_id
        )
        return self._request(Urls.db_delete, payload)

    def insert_tb_data(self, user_id, tb_id, fields, data):
        payload = dict(
            fields=json.dumps(fields),
            tb_id=tb_id,
            user_id=user_id,
            data=data if isinstance(data, basestring) else json.dumps(data)
        )
        return self._request(Urls.tb_data_insert, payload)

    def update_tb_data(self, user_id, tb_id, fields, data):
        payload = dict(
            fields=json.dumps(fields),
            tb_id=tb_id,
            user_id=user_id,
            data=data if isinstance(data, basestring) else json.dumps(data)
        )
        return self._request(Urls.tb_data_update, payload)

    def delete_tb_data(self, user_id, tb_id, fields, data):
        payload = dict(
            fields=json.dumps(fields),
            tb_id=tb_id,
            user_id=user_id,
            data=data if isinstance(data, basestring) else json.dumps(data)
        )
        return self._request(Urls.tb_data_delete, payload)

    def bulk_delete_tb_data(self, user_id, tb_id, where):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id,
            where=where
        )
        return self._request(Urls.tb_data_bulkdelete, payload)

    def set_tb_partition(self, user_id, tb_id, base_field, formula, param):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id,
            base_field=base_field,
            formula=formula,
            param=json.dumps(param)
        )
        return self._request(Urls.tb_partition_set, payload)

    def remove_tb_partition(self, user_id, tb_id):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id
        )
        return self._request(Urls.tb_partition_remove, payload)

    def clean_tb_data(self, user_id, tb_id):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id
        )
        return self._request(Urls.tb_data_clean, payload)

    def merge_tb_data(self, user_id, tb_id, is_fast=False):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id
        )
        if is_fast:
            payload['is_fast'] = 1
        return self._request(Urls.tb_data_merge, payload)

    def merge_tb_file(self, user_id, tb_id, fields, separator, null_holder):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id,
            fields=json.dumps(fields),
            separator=separator,
            null_holder=null_holder,
        )
        return self._request(Urls.tb_data_mergefile, payload)

    def preview_tb_data(self, tb_id, where={}, sort=[], where_linker='and', limit=1000, fields=[]):
        payload = dict(
            tb_id=tb_id,
            where=json.dumps(where),
            sort=json.dumps(sort),
            where_linker=where_linker,
            limit=limit,
            fields=json.dumps(fields),
        )
        return self._request(Urls.tb_data_preview, payload)

    def create_tb(self, user_id, name, type, fields, title='', db_id=None, storage_id=None):
        payload = dict(
            user_id=user_id,
            name=name,
            type=type,
            fields=json.dumps(fields),
            title=title,
            db_id=db_id,
            storage_id=storage_id
        )
        return self._request(Urls.tb_create, payload)

    def list_tb(self, user_id, db_id=None):
        payload = dict(
            user_id=user_id,
            db_id=db_id
        )
        return self._request(Urls.tb_list, payload)

    def refer_tb(self, tb_id):
        payload = dict(
            tb_id=tb_id
        )
        return self._request(Urls.tb_refer, payload)

    def refer_tbs(self, tb_ids):
        payload = dict(
            tb_id_list=json.dumps(tb_ids)
        )
        return self._request(Urls.tb_refers, payload)

    def valid_tb(self, tb_ids):
        payload = dict(
            tb_id_list=json.dumps(tb_ids)
        )
        return self._request(Urls.tb_valid, payload)

    def children_tb(self, tb_ids):
        payload = dict(
            tb_id_list=json.dumps(tb_ids)
        )
        return self._request(Urls.tb_children, payload)

    def rely_tb(self, tb_id):
        payload = dict(
            tb_id=tb_id
        )
        return self._request(Urls.tb_rely, payload)

    def rely_tbs(self, tb_ids):
        payload = dict(
            tb_ids=json.dumps(tb_ids)
        )
        return self._request(Urls.tb_relys, payload)

    def join_chain_tb(self, tb_id):
        payload = dict(
            tb_id=tb_id
        )
        return self._request(Urls.tb_joinchain, payload)

    def check_tb(self, tb_id):
        payload = dict(
            tb_id=tb_id
        )
        return self._request(Urls.tb_check, payload)

    def query_tb(self, tb_id, user_id, fields=[], where=[], group=[], order_by='', limit=50, cache=1, query_table='', temp_tables=[]):
        payload = dict(
            fields=json.dumps(fields),
            tb_id=tb_id,
            user_id=user_id,
            limit=limit,
            where=json.dumps(where),
            group=json.dumps(group),
            order_by=order_by,
            cache=cache,
            query_table=query_table,
            temp_tables=json.dumps(temp_tables),
        )
        return self._request(Urls.tb_query, payload)

    def adv_query_tb(self, tb_id, user_id, temp_tb_id, query_tables=None, cache=1, temp_tables=None, fields=None):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id,
            temp_tb_id=temp_tb_id,
            cache=cache,
            query_tables=json.dumps(query_tables or []),
            temp_tables=json.dumps(temp_tables or []),
            fields=json.dumps(fields or []),
        )
        return self._request(Urls.tb_adv_query, payload)

    def export(self, tb_id, user_id, temp_tb_id, task_id, query_tables=None, cache=1, temp_tables=None, fields=None):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id,
            temp_tb_id=temp_tb_id,
            cache=cache,
            task_id=task_id,
            query_tables=json.dumps(query_tables or []),
            temp_tables=json.dumps(temp_tables or []),
            fields=json.dumps(fields or [])
        )
        return self._request(Urls.db_query_export, payload)

    def ml_train_set_predict(self, tb_id, user_id, temp_tb_id, query_table, temp_tables, model_param, model_type,
                             dimension_count):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id,
            temp_tb_id=temp_tb_id,
            query_table=query_table,
            temp_tables=json.dumps(temp_tables),
            model_param=json.dumps(model_param),
            model_type=model_type,
            dimension_count=dimension_count,
        )
        return self._request(Urls.ml_train_set_predict, payload)

    def modify_tb(self, tb_id, db_id=None, title=None, status=None, storage_id=None, data_count=None, error_msg=None,
                  version=None, iVersion=None, tag=None, label=None, comment=None):
        payload = dict(
            tb_id=tb_id,
            db_id=db_id,
            title=title,
            status=status,
            storage_id=storage_id,
            data_count=data_count,
            error_msg=error_msg,
            version=version,
            iVersion=iVersion,
            tag=tag,
            label=label,
            comment=comment
        )
        return self._request(Urls.tb_modify, payload)

    def access_field(self, tb_id, user_id, field_id):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id,
            field_id=field_id,
        )
        return self._request(Urls.field_access, payload)

    def access_tb(self, tb_id, user_id, groups=[]):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id,
            groups=json.dumps(groups),
        )
        return self._request(Urls.tb_access, payload)

    def access_tb_batch(self, tb_list, user_id, groups=[]):
        payload = dict(
            tb_id=json.dumps(tb_list),
            user_id=user_id,
            groups=json.dumps(groups),
        )
        return self._request(Urls.tb_accessbatch, payload)

    def tb_sql_trans(self, tb_id, sql, user_id, group_ids=[], with_storage_id=1):
        payload = dict(
            tb_id=tb_id,
            sql=sql,
            user_id=user_id,
            groups=json.dumps(group_ids),
            with_storage_id=with_storage_id
        )
        return self._request(Urls.tb_sql_trans, payload)

    # 调用此接口前需要先进行tb权限验证
    def infos_tb(self, tb_ids):
        payload = dict(
            tb_ids=json.dumps(tb_ids),
        )
        return self._request(Urls.tb_infos, payload)

    def info_tb(self, tb_id, user_id, groups=[]):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id,
            groups=json.dumps(groups)
        )
        return self._request(Urls.tb_info, payload)

    def info_tb2(self, tb_id, user_id, groups=[]):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id,
            groups=json.dumps(groups)
        )
        return self._request(Urls.tb_info2, payload)

    def delete_tb(self, tb_id, user_id=None):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id
        )
        return self._request(Urls.tb_delete, payload)

    def cal_tb(self, fields, data, formula):
        payload = dict(
            fields=json.dumps(fields),
            data=json.dumps(data),
            formula=json.dumps(formula)
        )
        return self._request(Urls.tb_cal, payload)

    def share_tb(self, tb_id):
        payload = dict(
            tb_id=tb_id
        )
        return self._request(Urls.tb_share, payload)

    def stat_tb(self, user_id_list):
        payload = dict(
            user_id_list=json.dumps(user_id_list)
        )
        return self._request(Urls.tb_stat, payload)

    def copy_tb(self, tb_id, user_id):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id
        )
        return self._request(Urls.tb_copy, payload)

    def sync_tb(self, from_tb_id, to_tb_id):
        payload = dict(
            from_tb_id=from_tb_id,
            to_tb_id=to_tb_id
        )
        return self._request(Urls.tb_sync, payload)

    def create_workspace(self, tb_infos):
        """
        [{'tb_id:tb_id', 'user_id':user_id }]
        """
        payload = dict(
            tb_infos=tb_infos
        )
        return self._request(Urls.ws_create, payload)

    def create_shares(self, tb_infos):
        """
        [{'tb_id:tb_id', 'user_id':user_id, 'sharer': sharer }]
        """
        payload = dict(
            tb_infos=json.dumps(tb_infos)
        )
        return self._request(Urls.share_creates, payload)

    def create_share(self, sharer, tb_id, to_user='', to_group='', row_filter=[], col_filter=[], is_fixed=0):
        payload = dict(
            row_filter=json.dumps(row_filter),
            user_id=sharer,
            to_user=to_user,
            tb_id=tb_id,
            col_filter=json.dumps(col_filter),
            to_group=to_group,
            is_fixed=is_fixed
        )
        return self._request(Urls.share_create, payload)

    def list_share(self, user_id, groups=[]):
        payload = dict(
            user_id=user_id,
            groups=json.dumps(groups)
        )
        return self._request(Urls.share_list, payload)

    def info_share(self, tb_id, user_id=None):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id
        )
        return self._request(Urls.share_info, payload)

    def infos_of_share_tb(self, sh_ids, user_id, group_id_list):
        if isinstance(sh_ids, list):
            sh_ids = json.dumps(sh_ids)
        payload = dict(
            sh_ids=sh_ids,
            user_id=user_id,
            group_id_list=json.dumps(group_id_list)
        )
        return self._request(Urls.share_my_infos, payload)

    def info_share_to_me(self, tb_id, user_id, group_id_list):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id,
            group_id_list=json.dumps(group_id_list)
        )
        return self._request(Urls.share_my_info, payload)

    def list_share_of_my(self, user_id):
        payload = dict(
            user_id=user_id
        )
        return self._request(Urls.share_my_list, payload)

    def modify_share(self, sh_id, row_filter=None, col_filter=None):
        payload = dict(
            sh_id=sh_id
        )
        if row_filter is not None:
            payload['row_filter'] = json.dumps(row_filter)
        if col_filter is not None:
            payload['col_filter'] = json.dumps(col_filter)
        return self._request(Urls.share_modify, payload)

    def check_share(self, sh_id_list=[]):
        payload = dict(
            sh_id_list=json.dumps(sh_id_list)
        )
        return self._request(Urls.share_check, payload)

    def param_share(self, sh_id):
        payload = dict(
            sh_id=sh_id
        )
        return self._request(Urls.share_param, payload)

    def delete_share(self, sh_id_list=[]):
        """
        :param info: ["sh_xxx", ...]
        :return:
        """
        payload = dict(
            sh_id_list=json.dumps(sh_id_list)
        )
        return self._request(Urls.share_delete, payload)

    def modify_field(self, user_id, tb_id, field_id, title=None, uniq_index=None, type=None, seq_no=None,
                     aggregator=None, param=None, flag=None, remark=None):
        payload = dict(
            user_id=user_id,
            field_id=field_id,
            tb_id=tb_id,
            title=title,
            uniq_index=uniq_index,
            type=type,
            seq_no=seq_no,
            aggregator=aggregator,
            param=param,
            flag=flag,
            remark=remark
        )
        return self._request(Urls.field_modify, payload)

    def check_field(self, tb_id, fields):
        payload = dict(
            tb_id=tb_id,
            fields=json.dumps(fields)
        )
        return self._request(Urls.field_check, payload)

    def field_formula(self, tb_id, field_id, user_id=None):
        payload = dict(
            tb_id=tb_id,
            field_id=field_id,
            user_id=user_id
        )
        return self._request(Urls.field_formula, payload)

    def field_titlesync(self, from_tb_id, to_tb_id):
        payload = dict(
            s_tb_id=from_tb_id,
            t_tb_id=to_tb_id,
        )
        return self._request(Urls.field_titlesync, payload)

    def create_field(self, user_id, tb_id, name, type, title='', uniq_index=0, aggregator='', param='', flag=''):
        payload = dict(
            user_id=user_id,
            name=name,
            title=title if title else name,
            tb_id=tb_id,
            uniq_index=uniq_index,
            type=type,
            aggregator=aggregator,
            param=param,
            flag=flag
        )
        return self._request(Urls.field_create, payload)

    def valid_field(self, tb_id, aggregator, is_row=0, is_where=0, field_id=None, user_id=None):
        payload = dict(
            tb_id=tb_id,
            aggregator=aggregator,
            is_row=is_row,
            is_where=is_where,
            field_id=field_id,
            user_id=user_id
        )
        return self._request(Urls.field_valid, payload)

    def list_field(self, tb_id):
        payload = dict(
            tb_id=tb_id
        )
        return self._request(Urls.field_list, payload)

    def delete_field(self, tb_id, field_id, user_id=''):
        payload = dict(
            field_id=field_id,
            tb_id=tb_id,
            user_id=user_id
        )
        return self._request(Urls.field_delete, payload)

    def create_ds(self, user_id, db_id, name, type, host, port, username, password, database):
        payload = dict(
            username=username,
            db_id=db_id,
            name=name,
            database=database,
            host=host,
            user_id=user_id,
            password=password,
            type=type,
            port=port
        )
        return self._request(Urls.ds_create, payload)

    def info_ds(self, ds_id):
        payload = dict(
            ds_id=ds_id
        )
        return self._request(Urls.ds_info, payload)

    def list_ds(self, user_id):
        payload = dict(
            user_id=user_id
        )
        return self._request(Urls.ds_list, payload)

    def modify_ds(self, ds_id, name=None, type=None, host=None, port=None, username=None, password=None, database=None):
        payload = dict(
            ds_id=ds_id,
            name=name,
            type=type,
            host=host,
            port=port,
            username=username,
            password=password,
            database=database
        )
        return self._request(Urls.ds_modify, payload)

    def delete_ds(self, ds_id):
        payload = dict(
            ds_id=ds_id
        )
        return self._request(Urls.ds_delete, payload)

    def get_gen_info(self, gen_id):
        payload = dict(
            gen_id=gen_id
        )
        return self._request(Urls.gen_info, payload)

    def get_gen_info_by_tb(self, tb_id):
        payload = dict(
            tb_id=tb_id
        )
        return self._request(Urls.gen_info, payload)

    def profile_view(self, generator_info, type):
        payload = dict(
            generator_info=json.dumps(generator_info),
            type=type
        )
        return self._request(Urls.view_profile, payload)

    def union_append(self, tb_id, temp_tb_id, append_tb_id):
        payload = dict(
            tb_id=tb_id,
            temp_tb_id=temp_tb_id,
            append_tb_id=append_tb_id,
        )
        return self._request(Urls.union_append, payload)

    def update_union_table(self, tb_id, dep_tbs, to_user, extra_fields=[]):
        payload = dict(
           target_id=tb_id,
           target_user=to_user,
           tb_ids=json.dumps(dep_tbs),
           extra_fields=json.dumps(extra_fields)
        )
        return self._request(Urls.tb_union, payload)

    def create_view(self, user_id, table_name, type, generator_info, db_id=None):
        payload = dict(
            user_id=user_id,
            table_name=table_name,
            type=type,
            generator_info=json.dumps(generator_info),
            db_id=db_id
        )
        return self._request(Urls.view_create, payload)

    def copy_view(self, tb_id, user_id, tb_mapper):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id,
            tb_mapper=json.dumps(tb_mapper)
        )
        return self._request(Urls.view_copy, payload)

    def modify_view(self, tb_id, table_name, generator_info):
        payload = dict(
            tb_id=tb_id,
            table_name=table_name,
            generator_info=json.dumps(generator_info) if type(generator_info) is dict else generator_info
        )
        return self._request(Urls.view_modify, payload)

    def history_view(self, tb_id, storage_id):
        payload = dict(
            tb_id=tb_id,
            storage_id=storage_id
        )
        return self._request(Urls.view_history, payload)

    def diff_view(self, tb_id, generator_info):
        payload = dict(
            tb_id=tb_id,
            generator_info=json.dumps(generator_info)
        )
        return self._request(Urls.view_diff, payload)

    def preview_view(self, type, generator_info, limit=50):
        payload = dict(
            type=type,
            generator_info=json.dumps(generator_info),
            limit=limit
        )
        return self._request(Urls.view_preview, payload)

    def view_checksql(self, user_id, generator_info):
        payload = dict(
            user_id=user_id,
            generator_info=json.dumps(generator_info)
        )
        return self._request(Urls.view_checksql, payload)

    def view_gensql(self, type, generator_info, limit=50):
        payload = dict(
            type=type,
            generator_info=json.dumps(generator_info),
            limit=limit
        )
        return self._request(Urls.view_gensql, payload)

    def exists_view(self, tb_id, to_user):
        payload = dict(
            tb_id=tb_id,
            to_user=to_user
        )
        return self._request(Urls.view_exists, payload)

    def check_view(self, gen_id, view_status_read=0):
        payload = dict(
            gen_id=gen_id,
            view_status_read=view_status_read
        )
        return self._request(Urls.view_check, payload)

    def cal_retention(self, tb_id, data, query_table='', temp_tables=''):
        payload = dict(
            tb_id=tb_id,
            data=json.dumps(data),
            query_table=query_table,
            temp_tables=json.dumps(temp_tables),
        )
        return self._request(Urls.cal_retention, payload)

    def tb_usage(self, tb_id, chart_dep):
        payload = dict(
            tb_id=tb_id,
            chart_dep=chart_dep
        )
        return self._request(Urls.tb_usage, payload)

    def modify_tb_field_selected(self, tb_id, field_ids):
        payload = dict(
            tb_id=tb_id,
            field_ids=field_ids
        )
        return self._request(Urls.tb_field_selected_modify, payload)

    def query_tb_field_selected(self, tb_id):
        payload = dict(
            tb_id=tb_id
        )
        return self._request(Urls.tb_field_selected_query, payload)

    def field_filter(self, tb_id, field_id, filter_str=None, limit=50):
        payload = dict(
            tb_id=tb_id,
            field_id=field_id,
            limit=limit
        )
        if filter_str is not None:
            payload['filter_str'] = filter_str
        return self._request(Urls.filter_field, payload)


Urls = common.tools.enum(
    db_create='db/create',
    db_delete='db/delete',
    db_info='db/info',
    db_list='db/list',
    db_tb_list='db/tb/list',
    db_modify='db/modify',
    db_query_export='db/query/export',
    ds_create='ds/create',
    ds_delete='ds/delete',
    ds_info='ds/info',
    ds_list='ds/list',
    ds_modify='ds/modify',
    field_access='field/access',
    field_create='field/create',
    field_valid='field/valid',
    field_delete='field/delete',
    field_list='field/list',
    field_modify='field/modify',
    field_check='field/check',
    field_formula='field/formula',
    field_titlesync='field/titlesync',
    share_create='share/create',
    share_creates='share/creates',
    share_delete='share/delete',
    share_list='share/list',
    share_info='share/info',
    share_my_info='share/my/info',
    share_my_infos='share/my/infos',
    share_my_list='share/my/list',
    share_modify='share/modify',
    share_check='share/check',
    share_param='share/param',
    ws_create='ws/create',
    tb_create='tb/create',
    tb_delete='tb/delete',
    tb_access='tb/access',
    tb_accessbatch='tb/accessbatch',
    tb_sql_trans='tb/sqltrans',
    tb_info='tb/info',
    tb_info2='tb/info2',
    tb_infos='tb/infos',
    tb_refer='tb/refer',
    tb_refers='tb/refers',
    tb_valid='tb/valid',
    tb_children='tb/children',
    tb_rely='tb/rely',
    tb_relys='tb/relys',
    tb_joinchain='tb/joinchain',
    tb_list='tb/list',
    tb_modify='tb/modify',
    tb_query='tb/query',
    tb_adv_query='tb/advquery',
    tb_check='tb/check',
    tb_cal='tb/cal',
    tb_share='tb/share',
    tb_stat='tb/stat',
    tb_copy='tb/copy',
    tb_sync='tb/sync',
    tb_union='tb/union',
    view_create='view/create',
    view_check='view/check',
    view_checksql='view/checksql',
    view_copy='view/copy',
    view_modify='view/modify',
    view_preview='view/preview',
    view_history='view/history',
    view_diff='view/diff',
    view_exists='view/exists',
    view_profile='view/profile',
    union_append='view/unionappend',
    tb_data_delete='tb/data/delete',
    tb_data_clean='tb/data/clean',
    tb_data_insert='tb/data/insert',
    tb_data_merge='tb/data/merge',
    tb_data_mergefile='tb/data/mergefile',
    tb_data_preview='tb/data/preview',
    tb_data_update='tb/data/update',
    tb_data_bulkdelete='tb/data/bulkdelete',
    tb_partition_set='tb/partition/set',
    tb_partition_remove='tb/partition/remove',
    tb_field_selected_modify='tb/field_selected/modify',
    tb_field_selected_query='tb/field_selected/query',
    tb_usage='tb/usage',
    gen_info='gen/info',
    cal_retention='cal/retention',
    ml_train_set_predict='model/predict',
    filter_field='field/filter',
)


if __name__ == '__main__':
    import error_code
    try:
        tassadar = TassadarClient()
        result = tassadar.bulk_delete_tb_data('404f19b7b9360bd09bc250650e0f6058', 'tb_ea239451cacc427387cf2721adf0686c',
                                              ['fkb1d29bd5=\'采矿业\''])
        result = tassadar.merge_tb_data('404f19b7b9360bd09bc250650e0f6058', 'tb_ea239451cacc427387cf2721adf0686c')
        print result
    except TassadarException as e:
        print 'error: %s, %s' % (e, e.status)
        if e.status == error_code.TB_MERGE_ERROR:
            print 'gugude...'
