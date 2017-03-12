#!/usr/bin/python
# -*- coding: utf-8 -*-

import urllib2
import urllib
import json
import common.config
from util.http.request_context import HttpRequestContext


class AccountException(Exception):
    def __init__(self, status, errstr):
        self.status = status
        self.errstr = errstr


class Account():
    def __init__(self):
        self.overlord_url = common.config.get('overlord', 'url')
        self.context = HttpRequestContext.data
        self.ip = self.context.get("ip", '')
        self.ua = self.context.get("ua", '')
        self.imei = self.context.get("imei", '')
        self.uuid = self.context.get("uuid", '')
        self.trace_id = self.context.get("trace_id", '')

    def add_params(self, params):
        if not params:
            return params
        params["ip"] = self.ip
        params["ua"] = self.ua
        params["trace_id"] = self.trace_id
        params["imei"] = self.imei
        params["uuid"] = self.uuid
        return params

    def login(self, username, password, domain, system="", device_token="", verify_code='', app_ver=''):
        if not verify_code and not (username and password and domain):
            return False
        params = {
            "username": username,
            "password": password,
            "domain": domain,
            "system": system,
            "device_token": device_token,
            "verify_code": verify_code,
            "app_ver": app_ver,
        }
        params = self.add_params(params)
        url = '%s/api/account/login' % self.overlord_url
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def logout(self, access_token, device_token=""):
        if not access_token:
            return False

        url = '%s/api/account/logout' % self.overlord_url
        params = {
            "access_token": access_token,
            "device_token": device_token,
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def info(self, access_token, sub_id=""):
        if not access_token:
            return False

        url = '%s/api/account/info' % self.overlord_url
        params = {'access_token': access_token, "sub_id": sub_id}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def infos(self, access_token, user_id_list=[]):
        if not access_token:
            return False

        url = '%s/api/account/infos' % self.overlord_url
        params = {'access_token': access_token, "user_id_list": json.dumps(user_id_list)}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def token_info(self, access_token):
        if not access_token:
            return False

        url = '%s/api/access_token/info' % self.overlord_url
        params = {'access_token': access_token}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def device_token_list(self, user_list):
        if not user_list:
            return False

        params = {
            'user_id_list': json.dumps(user_list)
        }

        params = self.add_params(params)
        url = '%s/api/device_token/infos' % self.overlord_url
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def device_token_modify(self, access_token, system, device_token):
        if not (access_token and system and device_token):
            return False
        params = {
            'access_token': access_token,
            "system": system,
            "device_token": device_token
        }
        params = self.add_params(params)
        url = '%s/api/device_token/modify' % self.overlord_url
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def modify(self, access_token, modify_data='', sub_id=''):
        if not access_token or not modify_data:
            return False

        url = '%s/api/account/modify' % self.overlord_url
        params = {'access_token': access_token, 'data': modify_data, 'sub_id': sub_id}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def modify_personal(self, access_token, modify_data='', verify_code='', verify_type=1):
        if not access_token or not modify_data:
            return False

        url = '%s/api/account/modify_personal' % self.overlord_url
        params = {
            'access_token': access_token,
            'data': modify_data,
            'verify_code': verify_code,
            'verify_type': verify_type
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def modify_email(self, user_id, old_email, new_email):

        url = '%s/api/account/modify_email' % self.overlord_url
        params = {'user_id': user_id, 'old_email': old_email, 'new_email': new_email}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def modify_pwd(self, access_token, old_password, new_password):
        if not (access_token and old_password and new_password):
            return False

        url = '%s/api/account/modify_pwd' % self.overlord_url
        params = {
            'access_token': access_token,
            'old_password': old_password,
            'new_password': new_password
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def level_info(self, access_token):
        if not access_token:
            return False
        url = '%s/api/account/level_info' % self.overlord_url
        params = {'access_token': access_token}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def reset_pwd(self, access_token, sub_id, new_password):
        if not (access_token and sub_id and new_password):
            return False

        url = '%s/api/account/reset_pwd' % self.overlord_url
        params = {
            'access_token': access_token,
            'sub_id': sub_id,
            'new_password': new_password
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def activate(self, user_id):
        """邮箱激活"""
        if not user_id:
            return False

        url = '%s/api/account/activate' % self.overlord_url
        params = {'user_id': user_id}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def create(self, access_token, create_bag):
        if not (access_token and create_bag):
            return False

        url = '%s/api/account/create' % self.overlord_url
        create_bag["access_token"] = access_token
        create_bag = self.add_params(create_bag)

        request = urllib2.Request(url, urllib.urlencode(create_bag))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def delete(self, access_token, sub_id):
        if not (access_token and sub_id):
            return False

        url = '%s/api/account/delete' % self.overlord_url
        params = {
            'access_token': access_token,
            'sub_id': sub_id
        }

        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def list(self, access_token, offset=0, limit=1000):
        if not access_token:
            return False

        url = '%s/api/account/list' % self.overlord_url
        params = {
            'access_token': access_token,
            'offset': offset,
            'limit': limit
        }

        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def staff_limit(self, access_token):
        if not access_token:
            return False

        url = '%s/api/account/limit' % self.overlord_url
        params = {
            'access_token': access_token,
        }

        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def opends_token_gen(self, access_token):
        if not access_token:
            return False

        url = '%s/api/opends_token/gen' % self.overlord_url
        params = {'access_token': access_token}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def opends_token_get(self, access_token):
        if not access_token:
            return False

        url = '%s/api/opends_token/get' % self.overlord_url
        params = {'access_token': access_token}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def open_token_gen(self, username, password, domain):
        if not (username and password and domain):
            return False

        url = '%s/api/open_api_token/gen' % self.overlord_url
        params = {
            'username': username,
            'password': password,
            'domain': domain,
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def open_token_get(self, username, password, domain):
        if not (username and password and domain):
            return False

        url = '%s/api/open_api_token/get' % self.overlord_url
        params = {
            'username': username,
            'password': password,
            'domain': domain,
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def info_security(self, access_token):
        if not access_token:
            return False

        url = '%s/api/security/info' % self.overlord_url
        params = {'access_token': access_token}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def modify_security(self, access_token, share_inside_group, share_outside_group, ip_list, ip_strategy, device_strategy, remote_login_strategy, web_strategy, web_expire_setting, mob_expire_setting):
        if not access_token:
            return False

        url = '%s/api/security/modify' % self.overlord_url
        params = {
            "access_token": access_token, "share_inside_group": share_inside_group,
            "share_outside_group": share_outside_group, "ip_list": json.dumps(ip_list),
            "ip_strategy": ip_strategy, "device_strategy": device_strategy, "remote_login_strategy": remote_login_strategy,
            "web_strategy": web_strategy, "web_expire_setting": json.dumps(web_expire_setting), "mob_expire_setting": json.dumps(mob_expire_setting)
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def get_user_relation(self, own_id, user_ids):
        if not own_id or not user_ids:
            return False

        url = '%s/api/account/relation' % self.overlord_url
        params = {'own': own_id, 'user_ids': json.dumps(user_ids)}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def get_user_by_anonymous(self, user_id):
        if not user_id:
            return {}

        url = '%s/api/account/anonymous_info' % self.overlord_url
        params = {'user_id': user_id}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def check_management(self, access_token, user_id, admin_user_ids):
        if not user_id or not admin_user_ids:
            return False

        url = '%s/api/account/check_management' % self.overlord_url
        params = {'access_token': access_token, 'user_id': user_id, 'admin_user_ids': json.dumps(admin_user_ids)}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def modify_preview(self, access_token, modify_data='', sub_id=''):
        if not access_token or not modify_data:
            return False

        url = '%s/api/account/modify_preview' % self.overlord_url
        params = {'access_token': access_token, 'data': modify_data, 'sub_id': sub_id}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def send_notice(self, access_token, sub_id='', target=''):
        if not access_token or not sub_id:
            return False

        url = '%s/api/account/send_notice' % self.overlord_url
        params = {'access_token': access_token, 'sub_id': sub_id, 'target': target}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def init_password(self, sub_id='', password=''):
        if not sub_id or not password:
            return False

        url = '%s/api/account/init_pwd' % self.overlord_url
        params = {'sub_id': sub_id, 'password': password}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def init_pwd_info(self, sub_id=''):
        if not sub_id:
            return False

        url = '%s/api/account/init_pwd_info' % self.overlord_url
        params = {'sub_id': sub_id}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def get_enterprise_info(self, access_token, enterprise_id=''):
        if not enterprise_id or not access_token:
            return False

        url = '%s/api/account/enterprise_info' % self.overlord_url
        params = {'enterprise_id': enterprise_id}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def enterprise_user_list(self, access_token):
        """
        根据用户所属的企业域id，获取当前企业域下的所有用户的user_id
        :param access_token:
        :return:
        """
        if not access_token:
            return False
        url = '%s/api/account/enterprise_user_list' % self.overlord_url
        params = {'access_token': access_token}
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())
    
    def apply_retrieve_pwd(self, domain=None, username=None, verify_type=None, verify_code=None, session_id=None):
        """
        根据用户企业域和用户名来发起找回密码的请求,如果用户绑定了手机则自动向手机发送验证码,否则向用户邮箱发送重置密码链接.
        :param domain, username:
        :return:手机号或者邮箱
        """
        if not (domain and username and verify_type):
            return False
        url = '%s/api/account/apply_for_verify' % self.overlord_url
        params = {
            'domain': domain,
            'username': username,
            'verify_type': verify_type,
            'verify_code': verify_code,
            'session_id': session_id
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def retrieve_password(self, token=None, password=None):
        """
        设置新密码
        :param token, password
        :return
        """
        if not (token and password):
            return False
        url = '%s/api/account/retrieve_pwd' % self.overlord_url
        params = {
            'token': token,
            'password': password
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def verify_retrieve_token(self, token=None):
        """
        设置新密码
        :param token, password
        :return
        """
        if not token:
            return False
        url = '%s/api/account/retrieve_pwd' % self.overlord_url
        params = {
            'token': token
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def get_verify_pic(self, session_id=None):
        if not session_id:
            return False
        url = '%s/api/verify/pic' % self.overlord_url
        params = {
            'session_id': session_id
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return ret.read()

    def get_verify_sms(self, phone_num=None, usage=0, verify_code=None, session_id=None):
        if not phone_num:
            return False
        url = '%s/api/verify/sms' % self.overlord_url
        params = {
            'phone_num': phone_num,
            'usage': usage,
            'ip': self.ip,
            'verify_code': verify_code,
            'session_id': session_id
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def verify(self, username, verify_code, verify_type, session_id):
        # 个人版验证
        if not (username and verify_type):
            return 'username and verify_type required'

        url = '%s/api/verify/verify' % self.overlord_url
        params = {
            'username': username,
            'verify_code': verify_code.lower(),
            'verify_type': verify_type,
            'session_id': session_id,

        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def provision(self, username=None, domain=None, contact=None, password=None, source_token=None, staff_limit=None,
                  enterprise_type=None, mobile=None, email=None, data_size=None):
        if not (source_token and username and domain and contact):
            return False

        url = '%s/api/account/provision' % self.overlord_url
        params = {
            'source_token': source_token,
            'username': username,
            'domain': domain,
            'contact': contact,
        }

        if password is not None:
            params['password'] = password
        if staff_limit is not None:
            params['staff_limit'] = staff_limit
        if enterprise_type is not None:
            params['enterprise_type'] = enterprise_type
        if mobile is not None:
            params['mobile'] = mobile
        if email is not None:
            params['email'] = email
        if data_size is not None:
            params['data_size'] = data_size

        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def apply_retrieve_pwd_personal(self, domain, username, verify_type, verify_code, session_id):
        url = '%s/api/account/apply_for_verify_personal' % self.overlord_url
        params = {
            'username': username,
            'domain': domain,
            'verify_type': verify_type,
            'verify_code': verify_code,
            'session_id': session_id
        }
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def register(self, session_id, verify_code, register_type, username, password, invitation_code=''):

        # 邮箱注册需要verify_code，会话id以及邮箱
        if register_type == '1' and not (verify_code and username and session_id):
            return False
        # 手机注册需要手机密码
        if register_type == '2' and not (username and password):
            return False

        url = '%s/api/register/register' % self.overlord_url
        params = {
            'session_id': session_id,
            'register_type': register_type,
            'username': username,
            'password': password,
            'verify_code': verify_code.lower(),
            'invitation_code': invitation_code,
            'ip': self.ip
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def invitation_code(self, token, user_id, num='1'):
        url = '%s/api/verify/invitation_code' % self.overlord_url
        params = {
            'num': num,
            'user_id': user_id,
            'access_token': token
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def enterprise_modify(self, domain=None, source_token=None, staff_limit=None, data_size=None):
        if not (source_token and staff_limit and domain and data_size):
            return False

        url = '%s/api/account/enterprise_modify' % self.overlord_url
        params = {
            'source_token': source_token,
            'staff_limit': staff_limit,
            'domain': domain,
            'data_size': data_size,
            'is_del': 0
            }

        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def partner_register(self, domain, username, password, invitation_code):
        url = '%s/api/partner/register' % self.overlord_url
        params = {
            'domain': domain,
            'username': username,
            'password': password,
            'invitation_code': invitation_code,
            }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def partner_upgrade(self, enterprise_id, invitation_code):
        url = '%s/api/partner/upgrade' % self.overlord_url
        params = {
            'enterprise_id': enterprise_id,
            'invitation_code': invitation_code,
            }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    # 请求邀请码
    def invitation_code_request(self, name, company, tool, demand, position, access_token):
        url = '%s/api/verify/invitation_code_request' % self.overlord_url
        params = {
            'name': name,
            'company': company,
            'tool': tool,
            'demand': demand,
            'position': position,
            "access_token": access_token,
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def send_captcha(self, token, phone_num, usage, verify_code, session_id):
        url = '%s/api/captcha/send' % self.overlord_url
        params = {
            'access_token': token,
            'phone_num': phone_num,
            'usage': usage,
            "verify_code": verify_code, 
            "session_id": session_id
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def verify_captcha(self, token, usage, verify_code):
        url = '%s/api/captcha/verify' % self.overlord_url
        params = {
            'access_token': token,
            'verify_code': verify_code,
            'usage': usage,
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

    def verify_password(self, token, password):
        url = '%s/api/account/verify_pwd' % self.overlord_url
        params = {
            'access_token': token,
            'password': password,
        }
        params = self.add_params(params)
        request = urllib2.Request(url, urllib.urlencode(params))
        ret = urllib2.urlopen(request)
        return json.loads(ret.read())

if __name__ == '__main__':
    pass
