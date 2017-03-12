#!/usr/bin/python
# -*- coding: utf-8 -*-

from common.base_client import BaseClient
import common.tools


class FtsException(Exception):
    def __init__(self, errstr, status=1):
        self.errstr = errstr
        self.status = status

    def __str__(self):
        return self.errstr


class FtsClient(BaseClient):
    
    NORMAL_STATUS = '0'
    
    CONF_SECTION = "fts"
    KEY_NAME = "url"

    def __init__(self, **bag):
        BaseClient.__init__(self)
        self._pre_params = {
            "ip": bag.get('ip', ''),
            "ua": bag.get('ua', ''),
            "imei": bag.get('imei', ''),
            "uuid": bag.get('uuid', ''),
            "trace_id": bag.get('trace_id', '')
        }

    def pre_params(self):
        return self._pre_params

    def _request(self, short_uri, payload, files):

        res = self._do_request(short_uri, payload, files)
        result = res.json()
        if result['status'] != self.NORMAL_STATUS:
            raise FtsException(result['errstr'], int(result['status']))

        return result['result']
 
   
    def file_upload(self, storage_filename, file_io, payload={}):
        files = {'file': (storage_filename, file_io)}
        return self._request(Urls.file_upload, payload, files)
        
Urls = common.tools.enum(
    file_upload='api/file/upload',
    )
