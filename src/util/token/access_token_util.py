#!/usr/bin/python
# -*- coding: utf-8 -*-

from util.account.overlord_cli import OverlordCli

class AccessTokenUtil:

    def __init__(self, access_token=None, ip=None, ua=None):
        self.token = access_token
        self.ip = ip
        self.ua = ua
        self.token_info = {}

    def user_id(self):
        return self.token_info.get('user_id', '')

    def role(self):
        return self.token_info.get('role', 3)

    def domain(self):
        return self.token_info.get('domain', '')

    def enterprise_type(self):
        return self.token_info.get('user_info', {}).get('enterprise_type', -1)

    def user_info(self):
        return self.token_info.get('user_info', {})

    def is_admin(self):
        if self.role() in [0, 2]:
            return True
        return False

    def verify_token(self):
        """验证token 是否过期"""
        if not self.token:
            return False
        req = OverlordCli(self.ip).token_info(self.token)

        self.token_info = {
            "user_id": req['user_id'],
            "role": req['role'],
            "domain": req['domain'],
            "user_info": req['user_info'],
        }
        return True
