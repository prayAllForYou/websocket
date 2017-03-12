#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import common.tools
from common.base_client import BaseClient

class AthenaException(Exception):
    def __init__(self, errstr, status=1):
        self.errstr = errstr
        self.status = status

    def __str__(self):
        return self.errstr


class AthenaClient(BaseClient):
    """
    athena交互模块
    """
    NORMAL_STATUS = '0'

    CONF_SECTION = "athena"
    KEY_NAME = "url"

    def __init__(self):
        BaseClient.__init__(self)

    def _request(self, short_uri, payload):

        res = self._do_request(short_uri, payload)
        result = res.json()
        if result['status'] != self.NORMAL_STATUS:
            raise AthenaException(result['errstr'], int(result['status']))

        return result['result']

    def get_user_list(self):
        payload = dict(
        )
        return self._request(Urls.get_user_list, payload)

    def get_user_infos(self, id_list):
        payload = dict(
            id_list=json.dumps(id_list)
        )
        return self._request(Urls.get_user_infos, payload)

    def get_user_info(self, id):
        payload = dict(
            id=id
        )
        return self._request(Urls.get_user_info, payload)

    def get_auth_list(self, user_id):
        payload = dict(
            user_id=user_id
        )
        return self._request(Urls.get_auth_list, payload)

    def create_auth(self, user_id, manager_id, auth_start_time, time_type, duration, domain, company, username):
        payload = dict(
            user_id=user_id,
            manager_id=manager_id,
            auth_start_time=auth_start_time,
            time_type=time_type,
            duration=duration,
            domain=domain,
            company=company,
            username=username
        )
        return self._request(Urls.create_auth, payload)

    def modify_auth(self, auth_id, time_type, duration):
        payload = dict(
            auth_id=auth_id,
            time_type=time_type,
            duration=duration
        )
        return self._request(Urls.modify_auth, payload)

    def stop_auth(self, auth_id):
        payload = dict(
            auth_id=auth_id
        )
        return self._request(Urls.stop_auth, payload)

Urls = common.tools.enum(
    get_user_list='api/api/user/list',
    get_user_infos='api/user/infos',
    get_user_info='api/user/info',
    get_auth_list='api/authorization/list',
    create_auth='api/authorization/create',
    modify_auth='api/authorization/modify',
    stop_auth='api/authorization/stop'
)