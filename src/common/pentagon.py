#!/usr/bin/env python
# -*- coding:utf-8 -*-

import json
from common.base_client import BaseClient

def enum(**enums):
    return type('Enum', (), enums)


DsUrls = enum(
    view_exec='view/exec',
    view_update='view/update',
    view_cascade='view/cascade',
)


class PentagonException(Exception):
    def __init__(self, status, errstr, uri=""):
        self.status = status
        self.errstr = errstr
        self.uri = uri

    def __str__(self):
        return "<DSException: status %s, error %s, uri %s>" % (self.status, self.errstr, self.uri)


class PentagonClient(BaseClient):
    """
    DS交互模块
    """

    MODULE_NAME = "pentagon"

    CONF_SECTION = "pentagon"
    KEY_NAME = "url"

    def _request(self, short_uri, payload):

        res = self._do_request(short_uri, payload)

        repro_uri = "&".join(["%s=%s" % (key, payload[key]) for key in payload.keys() if payload[key] is not None])

        try:
            result = res.json()
        except:
            raise PentagonException("500", "no result returned from pentagon server", repro_uri)

        if result['status'] != "0":
            raise PentagonException(result['status'], result['errstr'], repro_uri)

        return result

    def exec_view(self, user_id, gen_id, tb_id=None, is_cascade=0):
        """执行合表任务"""
        params = {
            "user_id": user_id,
            "gen_id": gen_id,
            "tb_id": tb_id,
            "is_cascade": is_cascade
        }
        return self._request(DsUrls.view_exec, params)

    """
    以下两个接口为opends服务
    view_update 接口 是原opends中的ds.py 文件中的tb_update 接口

    """

    def view_update(self, user_id, tb_id=None, db_id=None):
        params = dict(
            user_id=user_id,
            tb_id=tb_id,
            db_id=db_id
        )

        return self._request(DsUrls.view_update, params)["result"]

    def view_cascade(self, user_id, tb_ids, force_update=None):
        params = dict(
            user_id=user_id,
            tb_ids=json.dumps(tb_ids),
            force_update=force_update
        )

        return self._request(DsUrls.view_cascade, params)["result"]

