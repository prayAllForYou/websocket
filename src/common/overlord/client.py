#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import common.tools
from common.base_client import BaseClient

class OverlordException(Exception):
    def __init__(self, errstr, status=1):
        self.errstr = errstr
        self.status = status

    def __str__(self):
        return self.errstr


class OverlordClient(BaseClient):
    """
    Overlord交互模块
    """
    NORMAL_STATUS = '0'
    MODULE_NAME = "overlord"

    CONF_SECTION = MODULE_NAME
    KEY_NAME = "url"

    def __init__(self, **bag):
        BaseClient.__init__(self)

        self._pre_params = {
            "ip": bag.get('ip', ''),
            "ua": bag.get('ua', ''),
            "imei": bag.get('imei', ''),
            "uuid": bag.get('uuid', ''),
            "trace_id": bag.get('trace_id', ''),
            "source": int(bag.get('source', 0)),
        }

    def pre_params(self):
        return self._pre_params

    def _request(self, short_uri, payload):

        res = self._do_request(short_uri, payload)

        if short_uri in JSON_FORMAT_IGNORE:
            return res.content

        result = res.json()
        if result['status'] != self.NORMAL_STATUS:
            raise OverlordException(result['errstr'], int(result['status']))

        return result['result']

    def login(self, username, password, domain, system="", device_token="", verify_code='', app_ver=''):
        payload = dict(
            username=username,
            password=password,
            domain=domain,
            system=system,
            device_token=device_token,
            verify_code=verify_code,
            app_ver=app_ver
        )
        return self._request(Urls.user_login, payload)

    def logout(self, access_token, device_token=""):
        payload = dict(
            access_token=access_token,
            device_token=device_token
        )
        return self._request(Urls.user_logout, payload)

    def info(self, user_id, sub_id=""):
        payload = dict(
            user_id=user_id,
            sub_id=sub_id
        )
        return self._request(Urls.user_info, payload)

    def infos(self, user_id, user_id_list=[]):
        payload = dict(
            user_id=user_id,
            user_id_list=json.dumps(user_id_list)
        )
        return self._request(Urls.user_infos, payload)

    def token_info(self, access_token, strict_mod=1):
        payload = dict(
            access_token=access_token,
            strict_mod=strict_mod
        )
        return self._request(Urls.token_info, payload)

    def token_create(self, user_id, system=''):
        payload = dict(
            user_id=user_id,
            system=system
        )
        return self._request(Urls.token_create, payload)

    def modify_theme(self, user_id, theme_id):
        payload = dict(
            user_id=user_id,
            theme_id=theme_id
        )
        return self._request(Urls.theme_modify, payload)

    def device_token_list(self, user_list):
        payload = dict(
            user_list=json.dumps(user_list)
        )
        return self._request(Urls.device_token_infos, payload)

    def device_token_modify(self, user_id, system, device_token):
        payload = dict(
            user_id=user_id,
            system=system,
            device_token=device_token
        )
        return self._request(Urls.device_token_modify, payload)

    def modify(self, user_id, modify_data='', sub_id=''):
        payload = dict(
            user_id=user_id,
            data=modify_data,
            sub_id=sub_id
        )
        return self._request(Urls.user_modify, payload)

    def modify_personal(self, user_id, modify_data='', verify_code='', verify_type=1):
        payload = dict(
            user_id=user_id,
            data=modify_data,
            verify_code=verify_code,
            verify_type=verify_type,
        )
        return self._request(Urls.personal_modify, payload)

    def modify_email(self, user_id, old_email, new_email):
        payload = dict(
            user_id=user_id,
            old_email=old_email,
            new_email=new_email
        )
        return self._request(Urls.email_modify, payload)

    def modify_pwd(self, user_id, old_password, new_password):
        payload = dict(
            user_id=user_id,
            old_password=old_password,
            new_password=new_password
        )
        return self._request(Urls.password_modify, payload)

    def level_info(self, user_id):
        payload = dict(
            user_id=user_id
        )
        return self._request(Urls.level_info, payload)

    def reset_pwd(self, user_id, sub_id, new_password):
        payload = dict(
            user_id=user_id,
            sub_id=sub_id,
            new_password=new_password
        )
        return self._request(Urls.reset_password, payload)

    def activate(self, user_id):
        payload = dict(
            user_id=user_id
        )
        return self._request(Urls.user_activate, payload)

    def create(self, user_id, create_bag):
        payload = dict(
            user_id=user_id
        )
        payload.update(create_bag)
        return self._request(Urls.user_create, payload)

    def delete(self, user_id, sub_id):
        payload = dict(
            user_id=user_id,
            sub_id=sub_id
        )
        return self._request(Urls.user_delete, payload)

    def list(self, user_id, offset=0, limit=1000, anonymous=0):
        payload = dict(
            user_id=user_id,
            offset=offset,
            limit=limit,
            anonymous=anonymous,
        )
        return self._request(Urls.user_list, payload)

    def staff_limit(self, user_id):
        payload = dict(
            user_id=user_id
        )
        return self._request(Urls.user_limit, payload)

    def open_token_gen(self, username, password, domain):
        payload = dict(
            username=username,
            password=password,
            domain=domain
        )
        return self._request(Urls.open_api_token_gen, payload)

    def open_token_get(self, username, password, domain):
        payload = dict(
            username=username,
            password=password,
            domain=domain
        )
        return self._request(Urls.open_api_token_get, payload)

    def info_security(self, user_id):
        payload = dict(
            user_id=user_id
        )
        return self._request(Urls.security_info, payload)

    def modify_security(self, user_id, security_info):
        payload = dict(
            user_id=user_id,
            security_info=json.dumps(security_info)
        )
        return self._request(Urls.security_modify, payload)

    def get_user_relation(self, own, user_ids):
        payload = dict(
            own=own,
            user_ids=json.dumps(user_ids)
        )
        return self._request(Urls.user_relation, payload)

    def get_user_by_anonymous(self, user_id, need_group=0):
        payload = dict(
            user_id=user_id,
            need_group=need_group
        )
        return self._request(Urls.user_info_by_anonymous, payload)

    def get_users_by_anonymous(self, user_id_list):
        payload = dict(
            user_id_list=json.dumps(user_id_list)
        )
        return self._request(Urls.user_infos_by_anonymous, payload)

    def check_management(self, user_id, sub_id, admin_user_ids):
        payload = dict(
            user_id=user_id,
            sub_id=sub_id,
            admin_user_ids=json.dumps(admin_user_ids)
        )
        return self._request(Urls.check_user_management, payload)

    def modify_preview(self, user_id, data='', sub_id=''):
        payload = dict(
            user_id=user_id,
            data=data,
            sub_id=sub_id
        )
        return self._request(Urls.user_modify_preview, payload)

    def send_notice(self, user_id, sub_id='', target=''):
        payload = dict(
            user_id=user_id,
            sub_id=sub_id,
            target=target
        )
        return self._request(Urls.user_send_notice, payload)

    def init_password(self, sub_id='', password=''):
        payload = dict(
            sub_id=sub_id,
            password=password
        )
        return self._request(Urls.user_password_init, payload)

    def init_pwd_info(self, sub_id=''):
        payload = dict(
            sub_id=sub_id
        )
        return self._request(Urls.password_init_info, payload)

    def get_enterprise_info(self, user_id, enterprise_id=''):
        payload = dict(
            user_id=user_id,
            enterprise_id=enterprise_id
        )
        return self._request(Urls.enterprise_info, payload)

    def enterprise_user_list(self, user_id):
        payload = dict(
            user_id=user_id,
        )
        return self._request(Urls.enterprise_user_list, payload)

    def apply_retrieve_pwd(self, domain=None, username=None, verify_type=None, verify_code=None, session_id=None):
        payload = dict(
            domain=domain,
            username=username,
            verify_type=verify_type,
            verify_code=verify_code,
            session_id=session_id
        )
        return self._request(Urls.apply_retrieve_pwd, payload)

    def retrieve_verify_code(self, domain=None, username=None, verify_code=None):
        payload = dict(
            domain=domain,
            username=username,
            verify_code=verify_code
        )
        return self._request(Urls.retrieve_verify_code, payload)

    def retrieve_password(self, token=None, password=None):
        payload = dict(
            token=token,
            password=password
        )
        return self._request(Urls.retrieve_password, payload)

    def verify_retrieve_token(self, token=None):
        payload = dict(
            token=token
        )
        return self._request(Urls.verify_retrieve_token, payload)

    def get_verify_pic(self, session_id=None):
        payload = dict(
            session_id=session_id
        )
        return self._request(Urls.get_verify_pic, payload)

    def get_verify_sms(self, phone_num=None, usage=0, verify_code=None, session_id=None):
        payload = dict(
            phone_num=phone_num,
            usage=usage,
            verify_code=verify_code,
            session_id=session_id
        )
        return self._request(Urls.get_verify_sms, payload)

    def verify(self, username, verify_code, verify_type, session_id):
        payload = dict(
            username=username,
            verify_code=verify_code.lower(),
            verify_type=verify_type,
            session_id=session_id
        )
        return self._request(Urls.personal_verify, payload)

    def verify_code_check(self, verify_code, verify_type, session_id=None):
        payload = dict(
            verify_code=verify_code.lower(),
            verify_type=verify_type,
            session_id=session_id
        )
        return self._request(Urls.verify_code_check, payload)

    def provision(self, username=None, domain=None, contact=None, password=None, source_token=None,
            staff_limit=None, enterprise_type=None, mobile=None, email=None, data_size=None):
        params = {
            "source_token": source_token,
            "username": username,
            "domain": domain,
            "contact": contact,
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
        payload = dict(
            params
        )
        return self._request(Urls.user_provision, payload)

    def apply_retrieve_pwd_personal(self, domain, username, verify_type, verify_code, session_id):
        payload = dict(
            domain=domain,
            username=username,
            verify_type=verify_type,
            verify_code=verify_code,
            session_id=session_id
        )
        return self._request(Urls.apply_retrieve_pwd_personal, payload)

    def register(self, session_id, verify_code, register_type, username, password, invitation_code=''):
        payload = dict(
            session_id=session_id,
            register_type=register_type,
            username=username,
            password=password,
            verify_code=verify_code.lower(),
            invitation_code=invitation_code,
        )
        return self._request(Urls.personal_register, payload)

    def invitation_code(self, user_id, num='1'):
        payload = dict(
            user_id=user_id,
            num=num
        )
        return self._request(Urls.invitation_code, payload)

    def enterprise_modify(self, enterprise_id, domain=None, source_token=None, staff_limit=None, data_size=None):
        payload = dict(
            enterprise_id=enterprise_id,
            source_token=source_token,
            staff_limit=staff_limit,
            domain=domain,
            data_size=data_size,
            is_del=0
        )
        return self._request(Urls.enterprise_modify, payload)

    def enterprise_init(self, user_id, enterprise_id, domain, password):
        payload = dict(
            enterprise_id=enterprise_id,
            user_id=user_id,
            domain=domain,
            password=password
        )
        return self._request(Urls.enterprise_init, payload)

    def partner_register(self, domain, username, password, invitation_code):
        payload = dict(
            domain=domain,
            username=username,
            password=password,
            invitation_code=invitation_code
        )
        return self._request(Urls.partner_register, payload)

    def partner_upgrade(self, enterprise_id, invitation_code, update_type):
        payload = dict(
            enterprise_id=enterprise_id,
            invitation_code=invitation_code,
            update_type=update_type
        )
        return self._request(Urls.partner_upgrade, payload)
    
    def partner_code_verity(self, invitation_code):
        payload = dict(
            invitation_code=invitation_code,
        )
        return self._request(Urls.partner_code_verity, payload)

    def invitation_code_request(self, name, company, tool, demand, position, user_id):
        payload = dict(
            name=name,
            company=company,
            tool=tool,
            demand=demand,
            position=position,
            user_id=user_id
        )
        return self._request(Urls.invitation_code_request, payload)

    def send_captcha(self, user_id, phone_num, usage, verify_code, session_id):
        payload = dict(
            user_id=user_id,
            phone_num=phone_num,
            usage=usage,
            verify_code=verify_code,
            session_id=session_id
        )
        return self._request(Urls.send_user_captcha, payload)

    def verify_captcha(self, user_id, access_token, usage, verify_code):
        payload = dict(
            user_id=user_id,
            access_token=access_token,
            usage=usage,
            verify_code=verify_code
        )
        return self._request(Urls.verify_user_captcha, payload)

    def verify_password(self, user_id, password):
        payload = dict(
            user_id=user_id,
            password=password
        )
        return self._request(Urls.verify_password, payload)

    def check_account(self, domain, username, password):
        payload = dict(
            domain=domain,
            username=username,
            password=password
        )
        return self._request(Urls.check_account, payload)

    """  *************  """
    """  ****GROUP****  """
    """  *************  """
    def group_create(self, user_id, group_name, add_users=""):
        payload = dict(
            user_id=user_id,
            name=group_name,
            add_users=add_users
        )
        return self._request(Urls.group_create, payload)

    def group_delete(self, user_id, group_id):
        payload = dict(
            user_id=user_id,
            group_id=group_id
        )
        return self._request(Urls.group_delete, payload)

    def group_list(self, user_id, detail="false"):
        payload = dict(
            user_id=user_id,
            detail=detail
        )
        return self._request(Urls.group_list, payload)

    def anonymous_group_list(self, user_id, group_list=[], is_all=0, detail="false"):
        payload = dict(
            user_id=user_id,
            detail=detail,
            group_id_list=json.dumps(group_list),
            is_all=is_all,
        )
        return self._request(Urls.anonymous_group_list, payload)

    def group_info(self, user_id, group_id):
        payload = dict(
            user_id=user_id,
            group_id=group_id
        )
        return self._request(Urls.group_info, payload)

    def group_infos(self, user_id, group_id_list=[]):
        payload = dict(
            user_id=user_id,
            group_id_list=json.dumps(group_id_list)
        )
        return self._request(Urls.group_infos, payload)

    def group_modify(self, user_id, group_id, group_name="", add_users="", del_users=""):
        payload = dict(
            user_id=user_id,
            group_id=group_id,
            group_name=group_name,
            add_users=add_users,
            del_users=del_users
        )
        return self._request(Urls.group_modify, payload)

    def group_user(self, group_ids):
        payload = dict(
            group_id_list=json.dumps(group_ids)
        )
        return self._request(Urls.group_user_list, payload)

    def remove_sub(self, user_id, group_id, sub_id):
        payload = dict(
            user_id=user_id,
            group_id=group_id,
            sub_id=sub_id
        )
        return self._request(Urls.group_remove_sub, payload)

    def gen_debug_token(self, user_id, admin_token, ttl, purpose):
        payload = dict(
            user_id=user_id,
            admin_token=admin_token,
            ttl=ttl,
            purpose=purpose
        )
        return self._request(Urls.gen_debug_token, payload)

    def ldap_login(self, username, password):
        payload = dict(
            username=username,
            password=password
        )
        return self._request(Urls.ldap_login, payload)

    def ldap_logout(self, access_token):
        payload = dict(
            access_token=access_token
        )
        return self._request(Urls.ldap_logout, payload)

    def list_debug_token(self, admin_token):
        payload = dict(
            admin_token=admin_token
        )
        return self._request(Urls.list_debug_token, payload)

    def ldap_token_info(self, access_token):
        payload = dict(
            access_token=access_token
        )
        return self._request(Urls.ldap_token_info, payload)

    def ip_check(self, ip):
        payload = dict(
            ip=ip
        )
        return self._request(Urls.ip_check, payload)

    """
        opends api
    """
    def opends_token_gen(self, user_id):
        payload = dict(
            user_id=user_id
        )
        return self._request(Urls.opends_token_gen, payload)

    def opends_token_get(self, user_id):
        payload = dict(
            user_id=user_id
        )
        return self._request(Urls.opends_token_get, payload)

    def opends_token_info(self, token):
        payload = dict(
            access_token=token
        )
        return self._request(Urls.opends_token_info, payload)

    def whitelist_info(self, user_id):
        payload = dict(
            user_id=user_id
        )
        return self._request(Urls.opends_whitelist_info, payload)

    def whitelist_modify(self, user_id, add_whitelist=[], del_whitelist=[], opends_ip_strategy=""):
        payload = dict(
            user_id=user_id,
            add_whitelist=json.dumps(add_whitelist),
            del_whitelist=json.dumps(del_whitelist),
            opends_ip_strategy=opends_ip_strategy
        )
        return self._request(Urls.opends_whitelist_modify, payload)

    def account_provision(self, domain, contact, mobile, email, username, password, staff_limit, data_size, enterprise_type, source_token=""):
        payload = dict(
            domain=domain,
            contact=contact,
            mobile=mobile,
            email=email,
            username=username,
            password=password,
            staff_limit=staff_limit,
            data_size=data_size,
            enterprise_type=enterprise_type,
            source_token=source_token
        )
        return self._request(Urls.account_provision, payload)

    def unsubscribe_email(self, user_id, email):
        payload = dict(
            user_id=user_id,
            email=email,

        )
        return self._request(Urls.unsubscribe_email, payload)
    
    def get_long_url(self, short_url):
        payload = dict(
            short_url=short_url,
        )
        return self._request(Urls.short_url, payload)

    def set_frozen(self, user_id, sub_id, is_frozen):
        payload = dict(
            user_id=user_id,
            sub_id=sub_id,
            is_frozen=is_frozen,

        )
        return self._request(Urls.set_frozen, payload)

    def opends_enterprise_info(self, opends_token, enterprise_id):
        payload = dict(
            enterprise_id=enterprise_id,
            access_token=opends_token
        )
        return self._request(Urls.opends_enterprise_info, payload)
    
    def guide_update(self, user_id, guide_flag):
        payload = dict(
            user_id=user_id,
            guide_flag=json.dumps(guide_flag),
        )
        return self._request(Urls.guide_update, payload)


    def get_openfe_token(self, username, password, domain):
        payload = dict(
            username=username,
            password=password,
            domain = domain
        )
        return self._request(Urls.openfe_access_token, payload)
    
    def openfe_user_frozen(self, user_id, sub_id):
        payload = dict(
            user_id=user_id,
            sub_id=sub_id
        )
        return self._request(Urls.openfe_user_frozen, payload)
    
    def openfe_user_thaw(self, user_id, sub_id):
        payload = dict(
            user_id=user_id,
            sub_id=sub_id
        )
        return self._request(Urls.openfe_user_thaw, payload)

    def pes_amount_op(self, user, op_type, op_amount, enterprise_id=None):
        payload = dict(
            user=user,
            op_type=op_type,
            op_amount=op_amount,
            enterprise_id=enterprise_id
        )
        return self._request(Urls.pes_op, payload=payload)


    def anonymous_user(self, domain, username):
        payload = dict(
            domain=domain,
            username=username
        )
        return self._request(Urls.anonymous_user, payload=payload)


    def create_auth_token(self, user_id, auth_id, manager_id, ttl):
        payload = dict(
            user_id=user_id,
            auth_id=auth_id,
            manager_id=manager_id,
            ttl=ttl
        )
        return self._request(Urls.auth_token_create, payload=payload)

    def modify_auth_token(self, user_id, auth_id, ttl):
        payload = dict(
            user_id=user_id,
            auth_id=auth_id,
            ttl=ttl
        )
        return self._request(Urls.auth_token_modify, payload=payload)

    def delete_auth_token(self, user_id, auth_id):
        payload = dict(
            user_id=user_id,
            auth_id=auth_id,
        )
        return self._request(Urls.auth_token_delete, payload=payload)

Urls = common.tools.enum(
    group_create='api/group/create',
    group_delete='api/group/delete',
    group_list='api/group/list',
    anonymous_group_list='api/group/anonymous_list',
    group_info='api/group/info',
    group_infos='api/group/infos',
    group_modify='api/group/modify',
    group_user_list='api/group/user_list',
    group_remove_sub='api/group/remove_sub',
    gen_debug_token='api/admin/gen_debug_token',
    ldap_login='api/admin/ldap_login',
    ldap_logout='api/admin/ldap_logout',
    list_debug_token='api/admin/list_debug_token',
    ldap_token_info='api/ldap_token/info',
    ip_check='api/security/ip_check',
    user_login='api/account/login',
    user_logout='api/account/logout',
    user_info='api/account/info',
    user_infos='api/account/infos',
    user_modify='api/account/modify',
    email_modify='api/account/modify_email',
    password_modify='api/account/modify_pwd',
    personal_modify='api/account/modify_personal',
    level_info='api/account/level_info',
    reset_password='api/account/reset_pwd',
    user_activate='api/account/activate',
    user_create='api/account/create',
    user_delete='api/account/delete',
    user_list='api/account/list',
    user_limit='api/account/limit',
    user_relation='api/account/relation',
    user_info_by_anonymous='api/account/anonymous_info',
    user_infos_by_anonymous='api/account/anonymous_infos',
    check_user_management='api/account/check_management',
    user_modify_preview='api/account/modify_preview',
    user_send_notice='api/account/send_notice',
    user_password_init='api/account/init_pwd',
    password_init_info='api/account/init_pwd_info',
    enterprise_info='api/account/enterprise_info',
    enterprise_modify='api/account/enterprise_modify',
    enterprise_init='api/account/enterprise_init',
    enterprise_user_list='api/account/enterprise_user_list',
    apply_retrieve_pwd='api/account/apply_for_verify',
    retrieve_verify_code='api/account/apply_for_verify',
    retrieve_password='api/account/retrieve_pwd',
    verify_retrieve_token='api/account/retrieve_pwd',
    user_provision='api/account/provision',
    apply_retrieve_pwd_personal='api/account/apply_for_verify_personal',
    verify_password='api/account/verify_pwd',
    check_account='api/account/check',
    token_info='api/access_token/info',
    token_create='api/access_token/create',
    theme_modify='api/account/modify_theme',
    pes_op='api/account/pes_op',
    device_token_infos='api/device_token/infos',
    device_token_modify='api/device_token/modify',
    opends_token_gen='api/opends_token/gen',
    opends_token_get='api/opends_token/get',
    opends_token_info='api/opends_token/info',
    opends_whitelist_info='api/opends_token/whitelist',
    opends_whitelist_modify='api/opends_token/modify_whitelist',
    opends_enterprise_info="api/opends_token/enterprise_info",
    open_api_token_gen='api/open_api_token/gen',
    open_api_token_get='api/open_api_token/get',
    security_info='api/security/info',
    security_modify='api/security/modify',
    get_verify_pic='api/verify/pic',
    get_verify_sms='api/verify/sms',
    personal_verify='api/verify/verify',
    verify_code_check='api/verify/check',
    invitation_code='api/verify/invitation_code',
    invitation_code_request='api/verify/invitation_code_request',
    personal_register='api/register/register',
    partner_register='api/partner/register',
    partner_upgrade='api/partner/upgrade',
    partner_code_verity='api/partner/code_verity',
    send_user_captcha='api/captcha/send',
    verify_user_captcha='api/captcha/verify',
    account_provision='api/account/provision',
    unsubscribe_email='api/account/unsubscribe_email',
    short_url="api/short_url",
    set_frozen='api/account/set_frozen',
    guide_update="api/account/guide_update",
    openfe_access_token="api/openfe/access_token",
    openfe_user_frozen="api/openfe/user_frozen",
    openfe_user_thaw="api/openfe/user_thaw",
    anonymous_user="api/account/anonymous_user",
    auth_token_create="api/auth_token/create",
    auth_token_modify="api/auth_token/modify",
    auth_token_delete="api/auth_token/delete",
)

JSON_FORMAT_IGNORE = ('api/verify/pic', )

if __name__ == '__main__':
    ol = OverlordClient()
    user = "yulongjiang"
    op_type = 1
    op_amount = 1234
    enterprise_id = "705fe0aba4d56e36df217113a71853df"
    result = ol.pes_amount_op(user=user, op_type=op_type, op_amount=op_amount, enterprise_id=enterprise_id)
    print result
