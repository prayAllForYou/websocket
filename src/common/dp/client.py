#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import requests
import common.config as config
import common.tools
import time
import base64
from requests.exceptions import ConnectionError


class DataParserException(Exception):
    def __init__(self, msg, status=1):
        self.msg = msg
        self.status = status

    def __str__(self):
        return self.msg


class DataParserClient():
    """
    DataParser交互模块
    """

    def __init__(self):
        self.url_prefix = config.get('service', 'data-parser')
        self.dp_mongo_urls = config.get('dp_mongo', 'dp_mongo_urls')
        self.dp_mongo_ports = config.get('dp_mongo', 'dp_mongo_ports')
        self.dp_file_db_name = config.get('dp_mongo', 'dp_mongo_ex_file_db_name')
        self.dp_data_db_name = config.get('dp_mongo', 'dp_mongo_ex_data_db_name')

    def _request(self, short_uri, payload):
        url = os.path.join(self.url_prefix, short_uri)
        res = None
        try_count = 0
        while True:
            try:
                res = requests.post(url, data=payload)
                break
            except ConnectionError as e:
                try_count += 1
                time.sleep(3)
                if try_count == 3:
                    raise DataParserException('data-parser connection error, request: %s, %s' % (url, e.message))
        try:
            result = res.json()
        except Exception, e:
            raise DataParserException('data-parser response error, request: %s, param: %s' % (url, e.message))
        if result['status'] != '0' or result['errstr']:
            raise DataParserException('data-parse response error, request: %s, result: %s' % (url, result))
        return result['result']

    def mongo_excel_parser(self, excel_id):
        payload = dict(
            service='mongo_excel_parse',
            excel_id=excel_id,
            mongo_urls=self.dp_mongo_urls,
            mongo_ports=self.dp_mongo_ports,
            file_db_name=self.dp_file_db_name,
            data_db_name=self.dp_data_db_name
        )
        return self._request(Urls.excel_parse, payload)

    def sync_excel_parser(self, excel_id, excel_file_data):
        payload = dict(
            service='sync_excel_parse',
            excel_id=excel_id,
            excel_data=base64.b64encode(excel_file_data),
        )
        return self._request(Urls.excel_parse, payload)

Urls = common.tools.enum(
    excel_parse='little-boy/service'
)
