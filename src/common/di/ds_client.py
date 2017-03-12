#!/usr/bin/env python
# -*- coding:utf-8 -*-

import json
import base64
from common.base_client import BaseClient
from common.pentagon import PentagonClient


def enum(**enums):
    return type('Enum', (), enums)


DsUrls = enum(
    ds_create='ds/create',
    ds_delete='ds/delete',
    ds_info='ds/info',
    ds_list='ds/list',
    ds_modify='ds/modify',
    ds_sync='ds/sync',
    ds_verify='ds/verify',
    ds_connect='ds/conn',
    ds_stat='ds/stat',
    ds_enigma='ds/enigma',
    public_list='ds/publiclist',
    pub_table_list='ds/pubtablelist',
    task_status='task/status',
    tb_remove='tb/remove',
    tb_refer_ds='tb/referds',
    ds_dstb='ds/dstb',
    ds_modifytb='ds/modifysync',
    ds_nslist='ds/nslist',
    view_update='view/update',
    view_cascade='view/cascade',
    shopex_list='shopex/list',
    shopex_create='shopex/create',
    shopex_bind='shopex/bind',
    shopex_token='shopex/token',
    modify_ds_field='field/dsfieldmodify',
    tb_list='field/tblist',
    field_save='field/save',
    ds_field_list='field/fieldlist',
    sem_list='sem/list',
    sem_refer='sem/tbrefer',
    sem_merge='sem/merge',
    bos_commit='bos/commit',
    sms_vcode='sms/vcode',
    sms_verify='sms/verify',
    sms_delete='sms/delete',
    sms_list='sms/list',
    sms_defaultbind='sms/defaultbind',
    sms_unbindall='sms/unbindall',
    kst_list='kst/list',
    kst_conn='kst/conn',
    kst_create='kst/create',
    kst_info='kst/info',
    kst_modify='kst/modify',
    rtapi_control='rtapi/control',
    rtapi_modify='rtapi/modify',
    rtapi_config='rtapi/config',
    rtapi_city='rtapi/city',
    rtapi_single_cost='rtapi/singlecost',
    rtapi_create='rtapi/create',
    rtapi_delete='rtapi/delete',
    rtapi_rtlist='rtapi/rtlist',
    rtapi_sync='rtapi/sync',
    sta_list='statistic/list',
    sta_add='statistic/add',
    sta_del='statistic/delete',
    sta_update='statistic/update',
    sta_ref='statistic/refer',
    sta_batch='statistic/batch',
    sta_token='statistic/token',
    export_large_file='export/largefile',
    rtapi_update_token='rtapi/updatetoken',
    mshopex_list='mshopex/list',
    mshopex_iscreate='mshopex/iscreate',

)


class DsException(Exception):

    def __init__(self, status, errstr, uri=""):
        self.status = status
        self.errstr = errstr
        self.uri = uri

    def __str__(self):
        return "<DSException: status %s, error %s, uri %s>" % (
            self.status, self.errstr, self.uri)


class DsClient(BaseClient):
    """
    DS交互模块
    """

    MODULE_NAME = "DI"

    CONF_SECTION = "di_server"
    KEY_NAME = "url"

    pentagon = PentagonClient()

    def _request(self, short_uri, payload):

        res = self._do_request(short_uri, payload)

        repro_uri = "&".join(["%s=%s" % (key, payload[key])
                              for key in payload.keys() if payload[key] is not None])

        try:
            result = res.json()
        except:
            raise DsException(
                "500",
                "no result returned from di server",
                repro_uri)

        if result['status'] != '0':
            raise DsException(result['status'], result['errstr'], repro_uri)

        return result

    def ex_import(self, user_id, file_data, file_name, config):
        """导入excel"""
        file_data = base64.b64encode(file_data)
        params = {
            "user_id": user_id,
            "file_name": file_name,
            "file_data": file_data,
            "config": json.dumps(config),
        }
        return self._request("excel/upload", params)

    def ex_download(self, user_id, excel_id):
        params = {
            "user_id": user_id,
            "excel_id": excel_id,
        }
        return self._request("excel/download", params)

    def ex_parser(self, excel_id, user_id, row_offsets, sheet_names):
        params = {
            "user_id": user_id,
            "excel_id": excel_id,
            'row_offsets': json.dumps(row_offsets),
            'sheet_names': json.dumps(sheet_names),
        }
        return self._request("excel/parser", params)

    def ex_preview(self, excel_id, user_id, row_offsets, sheet_names, limit):
        params = {
            "excel_id": excel_id,
            "user_id": user_id,
            "row_offsets": json.dumps(row_offsets),
            "sheet_names": json.dumps(sheet_names),
            "limit": limit,
        }
        return self._request("excel/preview", params)

    def ex_create(self, user_id, excel_id, sheet_names, tb_name, folder_id=''):
        params = {
            "user_id": user_id,
            "excel_id": excel_id,
            "sheet_names": sheet_names,
            "tb_name": tb_name,
            "folder_id": folder_id,
        }
        return self._request("excel/create", params)

    def ex_append(self, user_id, excel_id, sheet_name, tb_id, force, tb_title_ex_title_mapping, add_field_titles, del_field_titles):
        params = {
            "user_id": user_id,
            "excel_id": excel_id,
            "sheet_name": sheet_name,
            "tb_id": tb_id,
            'force': force,
            "tb_title_ex_title_mapping": json.dumps(tb_title_ex_title_mapping),
            "add_field_titles": json.dumps(add_field_titles),
            "del_field_titles": json.dumps(del_field_titles)
        }
        return self._request("excel/append", params)

    def ex_append_batch(self, user_id, tb_id, file_data, file_name, config):
        file_data = base64.b64encode(file_data)
        params = {
            'user_id': user_id,
            'tb_id': tb_id,
            'file_data': file_data,
            'file_name': file_name,
            'config': json.dumps(config),
        }
        return self._request('excel/appendbatch', params)

    def ex_replace(self, user_id, excel_id, sheet_name, tb_id, force, tb_title_ex_title_mapping, add_field_titles, del_field_titles):
        params = {
            "user_id": user_id,
            "excel_id": excel_id,
            "sheet_name": sheet_name,
            "tb_id": tb_id,
            'force': force,
            "tb_title_ex_title_mapping": json.dumps(tb_title_ex_title_mapping),
            "add_field_titles": json.dumps(add_field_titles),
            "del_field_titles": json.dumps(del_field_titles)
        }
        return self._request("excel/replace", params)

    def ex_replace_preview(self, user_id, excel_id, sheet_name,
                           tb_id, force, tb_title_ex_title_mapping, add_field_titles, del_field_titles):
        params = {
            "user_id": user_id,
            "excel_id": excel_id,
            "sheet_name": sheet_name,
            "tb_id": tb_id,
            'force': force,
            "tb_title_ex_title_mapping": json.dumps(tb_title_ex_title_mapping),
            "add_field_titles": json.dumps(add_field_titles),
            "del_field_titles": json.dumps(del_field_titles)
        }
        return self._request("excel/replacepreview", params)

    def ex_replace_one(self, user_id, excel_id, sheet_name,
                       tb_id, map_id, force, tb_title_ex_title_mapping, add_field_titles, del_field_titles):
        params = {
            "user_id": user_id,
            "excel_id": excel_id,
            "sheet_name": sheet_name,
            "tb_id": tb_id,
            "map_id": map_id,
            'force': force,
            "tb_title_ex_title_mapping": json.dumps(tb_title_ex_title_mapping),
            "add_field_titles": json.dumps(add_field_titles),
            "del_field_titles": json.dumps(del_field_titles)
        }
        return self._request("excel/replaceone", params)

    def ex_delete(self, user_id, tb_id, map_id):
        params = {
            "user_id": user_id,
            "tb_id": tb_id,
            'map_id': map_id,
        }
        return self._request("excel/delete", params)

    def ex_list(self, user_id, tb_id, query_type, page, is_whole):
        params = {
            "user_id": user_id,
            "tb_id": tb_id,
            "query_type": query_type,
            "page": page,
            "is_whole": is_whole,
        }
        return self._request("excel/list", params)

    def ex_execute(self, job_id, sheet_name):
        """导入excel"""
        params = {
            "job_id": job_id,
            "sheet_name": sheet_name,
        }
        return self._request("excel/execute", params)

    def ex_record(self, user_id, tb_id, offset, limit):
        """获取excel操作记录"""
        params = {
            "user_id": user_id,
            "tb_id": tb_id,
            "offset": offset,
            "limit": limit,
        }
        return self._request("excel/record", params)

    def task_status(self, task_id):
        params = {
            "task_id": task_id,
        }
        return self._request("task/status", params)

    def task_status_ex(self, user_id, task_id):
        params = {
            "user_id": user_id,
            "task_id": task_id,
        }
        return self._request("task/status", params)

    def field_modify(self, user_id, tb_id, fid, new_name):
        params = {
            "user_id": user_id,
            "tb_id": tb_id,
            "fid": fid,
            "new_name": new_name,
        }
        return self._request("field/modify", params)

    """
    数据源创建接口新增
    标签 和 文本计算字段
    参数 标签主要配合检索使用
    文本计算字段仅为第三方API服务
    这两个字段皆可缺省 不影响数据源的创建
    """

    def ds_create(self, user_id, type, ent_id, create_param, field_list='[]', tag=''):
        params = dict(
            user_id=user_id,
            type=type,
            ent_id=ent_id,
            create_param=create_param,
            field_list=field_list,
            tag=tag,
        )

        return self._request(DsUrls.ds_create, params)["result"]

    def ds_info(self, user_id, ds_id):
        params = dict(
            user_id=user_id,
            ds_id=ds_id,
        )

        return self._request(DsUrls.ds_info, params)["result"]

    def ds_delete(self, user_id, ds_id):
        params = dict(
            user_id=user_id,
            ds_id=ds_id
        )

        return self._request(DsUrls.ds_delete, params)["result"]

    """
    新增ds_nslist接口,为配合数据源界面检索与分页
    new params:
    : stype:搜索类型,0表示类型搜索,1表示名称或标签搜索
    : scontent:搜索内容,当stype为0时,取值为'0':全部数据源,
    : dstype:排序依据,0 数据源名称排序, 1 类型排序,2  更新时间排序,3 创建时间排序
    : sort_type:第二排序类型,0 正常顺序, 1 字典顺序, 2 字典逆序,
    : page:页数
    : sc_type:搜索依据的类型
    """

    def ds_nslist(
            self,
            user_id,
            dstype,
            scontent,
            stype,
            sort_type,
            page,
            sc_type):
        params = dict(
            user_id=user_id,
            dstype=dstype,
            scontent=scontent,
            stype=stype,
            sort_type=sort_type,
            page=page,
            sc_type=sc_type,
        )

        return self._request(DsUrls.ds_nslist, params)["result"]

    def ds_list(self, user_id):
        params = dict(
            user_id=user_id,
        )

        return self._request(DsUrls.ds_list, params)["result"]

    def tb_refer_ds(self, user_id, tb_id):
        params = dict(
            user_id=user_id,
            tb_id=tb_id
        )

        return self._request(DsUrls.tb_refer_ds, params)["result"]

    def pub_table_list(self):
        params = dict(
        )
        return self._request(DsUrls.pub_table_list, params)["result"]

    def ds_modify(self, ds_id, **kwargs):
        params = dict(
            ds_id=ds_id,
            **kwargs
        )

        return self._request(DsUrls.ds_modify, params)["result"]

    """
    测试接口增加对文本计算字段的校验
    """

    def ds_connect(self, user_id, type, conn_param, field_list=None):
        params = dict(
            user_id=user_id,
            type=type,
            conn_param=conn_param,
            field_list=field_list,
        )

        return self._request(DsUrls.ds_connect, params)["result"]

    def ds_verify(self, user_id, username, ds_type, token):
        params = dict(
            user_id=user_id,
            username=username,
            ds_type=ds_type,
            token=token
        )
        return self._request(DsUrls.ds_verify, params)["result"]

    def ds_sync(self, user_id, ds_id):
        params = dict(
            user_id=user_id,
            ds_id=ds_id
        )

        return self._request(DsUrls.ds_sync, params)["result"]

    def ds_stat(self, user_id_list):
        params = dict(
            user_id_list=user_id_list,
        )
        return self._request(DsUrls.ds_stat, params)["result"]

    def tb_remove(self, tb_id):
        params = dict(
            tb_id=tb_id
        )

        return self._request(DsUrls.tb_remove, params)["result"]

    def public_list(self, user_id):
        payload = dict(
            user_id=user_id
        )

        return self._request(DsUrls.public_list, payload)["result"]

    def ds_modifytb(self, ds_id, check_tables):
        params = dict(
            ds_id=ds_id,
            check_tables=check_tables
        )

        return self._request(DsUrls.ds_modifytb, params)["result"]

    def ds_cdstb(
            self,
            user_id,
            op_type,
            host,
            port,
            database,
            username,
            password,
            type):
        params = dict(
            user_id=user_id,
            op_type=op_type,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            type=type,
        )

        return self._request(DsUrls.ds_dstb, params)["result"]

    def ds_mdstb(self, ds_id, op_type):
        params = dict(
            ds_id=ds_id,
            op_type=op_type
        )

        return self._request(DsUrls.ds_dstb, params)["result"]

    """
    以下接口为商派服务
    """

    def shopex_token(self, user_id, code):
        params = dict(
            user_id=user_id,
            code=code,
        )
        return self._request(DsUrls.shopex_token, params)["result"]

    def shopex_list(self, user_id, shopex_id):
        params = dict(
            user_id=user_id,
            shopex_id=shopex_id,
        )
        return self._request(DsUrls.shopex_list, params)["result"]

    def shopex_create(self, user_id, enterprise_type, shop_list):
        params = dict(
            user_id=user_id,
            enterprise_type=enterprise_type,
            shop_list=shop_list,
        )
        return self._request(DsUrls.shopex_create, params)["result"]

    def shopex_bind(self, user_id):
        params = dict(
            user_id=user_id,
        )
        return self._request(DsUrls.shopex_bind, params)["result"]

    """
    数据源统一字段相关接口
    """

    def modify_ds_field(self, user_id, ds_id, fname, ftitle, fvalue):
        params = dict(
            user_id=user_id,
            ds_id=ds_id,
            fname=fname,
            ftitle=ftitle,
            fvalue=fvalue,
        )
        return self._request(DsUrls.modify_ds_field, params)["result"]

    def tb_list(self, user_id, ds_id, fname):
        params = dict(
            user_id=user_id,
            ds_id=ds_id,
            fname=fname,
        )
        return self._request(DsUrls.tb_list, params)["result"]

    def field_save(self, user_id, ds_id, field_list):
        params = dict(
            user_id=user_id,
            ds_id=ds_id,
            field_list=field_list,
        )
        return self._request(DsUrls.field_save, params)["result"]

    def ds_field_list(self, user_id, ds_id):
        params = dict(
            user_id=user_id,
            ds_id=ds_id,
        )
        return self._request(DsUrls.ds_field_list, params)["result"]

    # 新增Sem数据源追加合并接口

    def sem_list(self, user_id, ds_type, ds_id):
        params = dict(
            user_id=user_id,
            ds_type=ds_type,
            ds_id=ds_id,
        )
        return self._request(DsUrls.sem_list, params)["result"]

    def sem_refer(self, user_id, ds_id, tmp_ds_id):
        params = dict(
            user_id=user_id,
            ds_id=ds_id,
            tmp_ds_id=tmp_ds_id,
        )
        return self._request(DsUrls.sem_refer, params)["result"]

    def sem_merge(self, user_id, link_list):
        params = dict(
            user_id=user_id,
            link_list=link_list,
        )
        return self._request(DsUrls.sem_merge, params)["result"]

    def bos_commit(self, user_id, tb_id, param):
        params = dict(
            user_id=user_id,
            tb_id=tb_id,
            params=json.dumps(param)
        )
        return self._request(DsUrls.bos_commit, params)["result"]

    def sms_vcode(self, user_id, phone, new_phone):
        params = dict(
            user_id=user_id,
            phone=phone,
            new_phone=new_phone,
        )
        return self._request(DsUrls.sms_vcode, params)["result"]

    def sms_verify(self, user_id, phone, new_phone, vcode):
        params = dict(
            user_id=user_id,
            phone=phone,
            new_phone=new_phone,
            vcode=vcode,
        )
        return self._request(DsUrls.sms_verify, params)["result"]

    def sms_delete(self, user_id, phone):
        params = dict(
            user_id=user_id,
            phone=phone,
        )
        return self._request(DsUrls.sms_delete, params)["result"]

    def sms_list(self, user_id):
        params = dict(
            user_id=user_id,
        )
        return self._request(DsUrls.sms_list, params)["result"]

    def sms_defaultbind(self, user_id, phone):
        params = dict(
            user_id=user_id,
            phone_number=phone
        )
        return self._request(DsUrls.sms_defaultbind, params)["result"]

    def sms_unbindall(self, user_id):
        params = dict(
            user_id=user_id
        )
        return self._request(DsUrls.sms_unbindall, params)["result"]

    def kst_list(self, user_id, compId):
        params = dict(
            user_id=user_id,
            compId=compId,
        )
        return self._request(DsUrls.kst_list, params)["result"]

    def kst_conn(
            self,
            user_id,
            db_type,
            url,
            compId,
            ak,
            at,
            ase,
            name,
            field_list='[]'):
        params = dict(
            user_id=user_id,
            db_type=db_type,
            url=url,
            compId=compId,
            ak=ak,
            at=at,
            ase=ase,
            name=name,
            field_list=field_list,
        )
        return self._request(DsUrls.kst_conn, params)["result"]

    def kst_create(
            self,
            user_id,
            db_type,
            url,
            compId,
            ak,
            at,
            ase,
            name,
            field_list='[]',
            tag=''):
        params = dict(
            user_id=user_id,
            db_type=db_type,
            url=url,
            compId=compId,
            ak=ak,
            at=at,
            ase=ase,
            name=name,
            field_list=field_list,
            tag=tag,
        )
        return self._request(DsUrls.kst_create, params)["result"]

    def kst_info(self, ds_id):
        params = dict(
            ds_id=ds_id,
        )
        return self._request(DsUrls.kst_info, params)["result"]

    def kst_modify(self, ds_id, url, compId, ak, at, ase):
        params = dict(
            ds_id=ds_id,
            url=url,
            compId=compId,
            ak=ak,
            at=at,
            ase=ase,
        )
        return self._request(DsUrls.kst_modify, params)["result"]

    def ds_enigma(self, user_id, ent_id, name, type):
        params = dict(
                user_id=user_id,
                ent_id=ent_id,
                name=name,
                type=type,
                )
        return self._request(DsUrls.ds_enigma, params)["result"]

    """
    百度实况监控接口
    """

    def realtime_ds_create(self, bag):
        params = dict(
            user_id=bag.get('user_id'),
            name=bag.get('name'),
            type=bag.get('db_type'),
            bdp_access_token=bag.get('bdp_access_token'),
            ent_id=bag.get('ent_id')
        )

        return self._request(DsUrls.ds_create, params)["result"]

    def realtime_api_create(self, bag):
        params = dict(
            user_id=bag.get('user_id'),
            name=bag.get('name'),
            ds_id=bag.get('ds_id'),
            search_words=bag.get('search_words'),
            citys=bag.get('citys'),
            frequency=bag.get('frequency'),
            device=bag.get('device'),
            start_time=bag.get('start_time'),
            end_time=bag.get('end_time'),
            time_limit=bag.get('time_limit'),
            rt_type=bag.get('rt_type')
        )

        return self._request(DsUrls.rtapi_create, params)["result"]

    def realtime_api_config(self, bag):
        params = dict(
            user_id=bag.get('user_id'),
            ds_id=bag.get('ds_id'),
            realtime_id=bag.get('realtime_id')
        )
        return self._request(DsUrls.rtapi_config, params)["result"]

    def realtime_api_modify(self, bag):
        params = dict(
            realtime_id=bag.get('realtime_id'),
            ds_id=bag.get('ds_id'),
            modify=bag.get('modify'),
            user_id=bag.get('user_id'),
            name=bag.get('name'),
            search_words=bag.get('search_words'),
            citys=bag.get('citys'),
            frequency=bag.get('frequency'),
            device=bag.get('device'),
            start_time=bag.get('start_time'),
            end_time=bag.get('end_time'),
            time_limit=bag.get('time_limit'),
            rt_type=bag.get('rt_type')
        )

        return self._request(DsUrls.rtapi_modify, params)["result"]

    def realtime_api_control(self, bag):
        params = dict(
            ds_id=bag.get('ds_id'),
            user_id=bag.get('user_id'),
            is_pause=bag.get('is_pause'),
            realtime_id=bag.get('realtime_id')
        )
        return self._request(DsUrls.rtapi_control, params)["result"]

    def realtime_api_city(self, bag):
        params = dict(
            user_id=bag.get('user_id')
        )
        return self._request(DsUrls.rtapi_city, params)["result"]

    def realtime_api_cost(self, bag):
        params = dict(
            ds_id=bag.get('ds_id'),
            user_id=bag.get('user_id'),
            realtime_id=bag.get('realtime_id')
        )
        return self._request(DsUrls.rtapi_single_cost, params)["result"]

    def realtime_api_rtlist(self, bag):
        params = dict(
            ds_id=bag.get('ds_id'),
            user_id=bag.get('user_id')
        )
        return self._request(DsUrls.rtapi_rtlist, params)["result"]

    def realtime_api_delete(self, bag):
        params = dict(
            ds_id=bag.get('ds_id'),
            user_id=bag.get('user_id'),
            realtime_id=bag.get('realtime_id')
        )
        return self._request(DsUrls.rtapi_delete, params)["result"]

    def realtime_api_sync(self, bag):
        params = dict(
            ds_id=bag.get('ds_id'),
            user_id=bag.get('user_id'),
            realtime_id=bag.get('realtime_id')
        )
        return self._request(DsUrls.rtapi_sync, params)["result"]

    def export_large_file(self, tb_id, user_id, temp_tb_id, query_tables=None, cache=1, temp_tables=None, fields=None):
        payload = dict(
            tb_id=tb_id,
            user_id=user_id,
            temp_tb_id=temp_tb_id,
            cache=cache,
            query_tables=json.dumps(query_tables or []),
            temp_tables=json.dumps(temp_tables or []),
            fields=json.dumps(fields or [])
            )
        return self._request(DsUrls.export_large_file, payload)

    def sta_list(self, ds_id):
        params = dict(
                ds_id=ds_id,
                )
        return self._request(DsUrls.sta_list, params)["result"]

    def sta_add(self, user_id, ds_id, net_domain):
        params = dict(
                user_id = user_id,
                ds_id=ds_id,
                net_domain=net_domain,
                )
        return self._request(DsUrls.sta_add, params)["result"]

    def sta_del(self, ds_id, net_domain):
        params = dict(
                ds_id=ds_id,
                net_domain=net_domain,
                )
        return self._request(DsUrls.sta_del, params)["result"]

    def sta_update(self, ds_id, net_domain, config_list):
        params = dict(
                ds_id=ds_id,
                net_domain=net_domain,
                config_list=config_list,
                )
        return self._request(DsUrls.sta_update, params)["result"]

    def sta_ref(self, ds_id, net_domain):
        params = dict(
                ds_id=ds_id,
                net_domain=net_domain,
                )
        return self._request(DsUrls.sta_ref, params)["result"]

    def sta_batch(self, ds_id):
        params = dict(
                ds_id=ds_id,
                )
        return self._request(DsUrls.sta_batch, params)["result"]

    def sta_token(self, ds_id):
        params = dict(
                ds_id=ds_id,
                )
        return self._request(DsUrls.sta_token, params)["result"]
        payload = dict(
            tb_id=tb_id,
            user_id=user_id,
            temp_tb_id=temp_tb_id,
            cache=cache,
            query_tables=json.dumps(query_tables or []),
            temp_tables=json.dumps(temp_tables or []),
            fields=json.dumps(fields or [])
        )
        return self._request(DsUrls.export_large_file, payload)

    def realtime_api_update_token(self, bag):
        params = dict(
            user_id=bag.get('user_id'),
            token=bag.get('token')
        )
        return self._request(DsUrls.rtapi_update_token, params)["result"]

    def mshopex_list(self, user_id, shopex_id, access_token):
        params = dict(
                user_id=user_id,
                shopex_id=shopex_id,
                access_token=access_token,
                )
        return self._request(DsUrls.mshopex_list, params)["result"]

    def mshopex_iscreate(self, user_id):
        params = dict(
                user_id=user_id,
                )
        return self._request(DsUrls.mshopex_iscreate, params)["result"]
