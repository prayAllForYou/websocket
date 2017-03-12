#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import requests
import json
import base64
import time
import common.config
from requests.exceptions import ConnectionError
from common.pentagon import PentagonClient
# from util.log.log import trace_info

reload(sys)
sys.setdefaultencoding('utf-8')


class DiClientException(Exception):
    def __init__(self, msg, status=1):
        self.msg = msg
        self.status = status

    def __str__(self):
        return self.msg


class DiClient():
    """
        Di Client
    """
    NORMAL_STATUS = '0'

    pentagon = PentagonClient()

    def __init__(self):
        self.url_prefix = common.config.get('service', 'di_service').split(',')

    def _request(self, url, params):
        base_urls = self.url_prefix
        try_count = 0
        base_urls_len = len(base_urls)

        while try_count < 30:
            prefix_url = base_urls[try_count % base_urls_len]
            full_url = os.path.join(prefix_url, url)

            try:
                # request_start_time = time.time()
                res = requests.post(full_url, data=params)
                # request_end_time = time.time()
                # cost_time = int((request_end_time - request_start_time)*1000)
                # trace_info("di api [%s], cost time : %s " % (full_url, cost_time))
                break
            except ConnectionError as e:
                try_count += 1
                time.sleep(2)
                print 'can not connect di, retry...'
                if try_count == 30:
                    raise e

        result = res.json()
        if result['status'] != self.NORMAL_STATUS:
            raise DiClientException(result['errstr'], int(result['status']))
        return result['result']

    """接口"""

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

    def ex_create(self, user_id, excel_id, sheet_names, tb_name, folder_id=''):
        params = {
            "user_id": user_id,
            "excel_id": excel_id,
            "sheet_names": sheet_names,
            "tb_name": tb_name,
            "folder_id": folder_id,
        }
        return self._request("excel/create", params)

    def ex_append(self, user_id, excel_id, sheet_name, tb_id, force='0'):
        params = {
            "user_id": user_id,
            "excel_id": excel_id,
            "sheet_name": sheet_name,
            "tb_id": tb_id,
            'force': force,
        }
        return self._request("excel/append", params)

    def ex_info(self, user_id, excel_id):
        params = {
            "user_id": user_id,
            "excel_id": excel_id,
        }
        return self._request("excel/info", params)

    def ex_data(self, user_id, excel_id, sheet_name, schema):
        params = {
            "user_id": user_id,
            "excel_id": excel_id,
            "sheet_name": sheet_name,
            "schema": json.dumps(schema),
        }
        return self._request("excel/exdata", params)

    def ex_append_batch(self, user_id, tb_id, file_data, file_name, config):
        file_data = base64.b64encode(file_data)
        params = {
                'user_id':      user_id,
                'tb_id':        tb_id,
                'file_data':    file_data,
                'file_name':    file_name,
                'config':       json.dumps(config),
        }
        return self._request('excel/appendbatch', params)

    def ex_replace(self, user_id, excel_id, sheet_name, tb_id, force='0'):
        params = {
            "user_id": user_id,
            "excel_id": excel_id,
            "sheet_name": sheet_name,
            "tb_id": tb_id,
            'force': force,
        }
        return self._request("excel/replace", params)

    def ex_replace_one(self, user_id, excel_id, sheet_name, tb_id, map_id, force='0'):
        params = {
            "user_id": user_id,
            "excel_id": excel_id,
            "sheet_name": sheet_name,
            "tb_id": tb_id,
            "map_id": map_id,
            'force': force,
        }
        return self._request("excel/replaceone", params)

    def ex_delete(self, user_id, tb_id, map_id):
        params = {
            "user_id": user_id,
            "tb_id": tb_id,
            'map_id': map_id,
        }
        return self._request("excel/delete", params)

    def ex_list(self, user_id, tb_id):
        params = {
            "user_id": user_id,
            "tb_id": tb_id,
        }
        return self._request("excel/list", params)

    """    OLD     """
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

    def task_status(self, user_id, task_id):
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

if __name__ == "__main__":
    pass
