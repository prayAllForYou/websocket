#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
from hashlib import md5
from random import randint
from util.account.overlord_cli import OverlordCli
from helper.tassadar.tassadar_helper import TassadarClient
from util.metautil.metahelper import MetaHelper
import common.config


def user_info(token, hide_mobile=True):
    res = {}
    if not token:
        return res

    req_res = OverlordCli().info(token)
    res['username'] = req_res['username']
    res['user_id'] = req_res['user_id']
    res['mobile'] = req_res['mobile']
    res['contact'] = req_res['name']
    res['email'] = req_res['email']
    res['verify_email'] = req_res['verify_email']
    res['role'] = req_res['role']
    res['account_permission'] = req_res['account_permission']
    res['data_permission'] = req_res['data_permission']
    res['share_inside_group'] = req_res['security_info']['share_inside_group']
    res['share_outside_group'] = req_res['security_info']['share_outside_group']
    res['enterprise_type'] = int(req_res.get('enterprise_type', 0))
    res['photo_id'] = req_res['photo_id']
    res['guide'] = int(req_res['guide'])
    res['domain'] = req_res['domain']
    res['enterprise_type'] = req_res['enterprise_type']
    res['should_complete_profile'] = req_res['should_complete_profile']
    res['security_info'] = {
        "share_inside_group": req_res['security_info']['share_inside_group'],
        "share_outside_group": req_res['security_info']['share_outside_group'],
        "watermark_strategy": req_res['security_info']['watermark_strategy']
    }
    res['theme_id'] = req_res.get('theme_id', '')
    res['new_edition'] = int(req_res['new_edition'])
    # format
    if hide_mobile:
        res['mobile'] = format_mobile(res['mobile'])
    if res['domain'] == 'personal':
        if res['email'].find('@') > 3:
            res['email'] = res['email'][0:3] + '****' + res['email'][res['email'].find('@'):]
        if res['username'].find('@') > 3:
            res['username'] = res['username'][0:3] + '****' + res['username'][res['username'].find('@'):]
        elif len(res['username']) == 11:
            res['username'] = res['username'] = res['username'][0:3] + '****' + res['username'][7:]
        if int(res['enterprise_type']) == 3:
            res['personal_info'] = req_res['personal_info']
    if res['enterprise_type'] == 5:
        res['is_init'] = req_res['is_init']

    return res

def format_mobile(mobile):
    if not mobile:
        return ""
    base = "****"
    mobile = str(mobile)
    return mobile[0:3] + base + mobile[7:]

def share_user_list(token):
    if not token:
        return False, False

    req_res = OverlordCli().level_info(token)
    all_user = req_res.get('user', [])
    all_group = req_res.get('group', [])
    return all_user, all_group


def share_user_info(token, user_id):
    # 废弃
    if not token or not user_id:
        return {}

    req_res = OverlordCli().get_user_by_anonymous(user_id)
    return req_res

def anonymous_user_info(user_id, need_group = 0):
    if not user_id:
        return {}

    try:
        req_res = OverlordCli().get_user_by_anonymous(user_id, need_group)
    except:
        return {}
    return req_res

def anonymous_user_infos(user_id_list):
    if not user_id_list:
        return {}

    try:
        req_res = OverlordCli().get_users_by_anonymous(user_id_list)
    except:
        return {}
    return req_res

def belong_group(token, user_id=''):
    res = []
    if not token:
        if user_id:
            req_res = OverlordCli().get_user_by_anonymous(user_id, 1)
            res = req_res['belong_group']
        return res

    req_res = OverlordCli().info(token)
    return req_res.get('groups', [])


def mail_param(type, prefix):
    """1:个人版  2：企业版"""
    res = {}
    if type == 1:
        res['team'] = common.config.get('register_mail', 'person_team')
        res['domain'] = common.config.get('register_mail', 'person_domain')
        res['email'] = common.config.get('register_mail', 'person_email')
    else:
        res['team'] = common.config.get('register_mail', 'enterprise_team')
        res['domain'] = common.config.get('register_mail', 'enterprise_domain')
        res['email'] = common.config.get('register_mail', 'enterprise_email')
    res['activecode'] = gen_code(prefix)
    return res


def gen_code(prefix):
    return "%s" % (md5("%s_%s_%s" % (prefix, time.time(), randint(0, 100000))).hexdigest())


def sub_list(token):
    res = []
    if not token:
        return res

    req_res = OverlordCli().list(token)
    for i in req_res:
        res.append({"userid": i["user_id"], "role": i["role"], "name": i["name"], "username": i['username']})
    return res

def remove_account_from_group_cleanup(access_token, sub_id):
    if not sub_id:
        return False

    # # # 数据清理 如果一个账户的分组被更改，那么原来的管理员可能不能再管理这个用户，那么以前这个管理员分配的规则以及工作表都要全部清理
    admin_user_ids = []
    assigned_work_table = TassadarClient().list_share(sub_id)

    # 把四次元空间袋里的分享排除
    assigned_work_table = [share for share in assigned_work_table if share['is_fixed'] == 0]
    for i in assigned_work_table:
        if i["target"] == 0:
            if not i["sharer"].startswith('ws_'):
                admin_user_ids.append(i["sharer"])

    admin_user_ids = list(set(admin_user_ids))
    if admin_user_ids:
        res = OverlordCli().check_management(access_token, sub_id, admin_user_ids)
        check_res = res
        share_delete_list = []
        for check in check_res:
            if check["relation"] == 0:
                share_delete_list += [i["sh_id"] for i in assigned_work_table if
                                      i["target"] == 0 and i["sharer"] == check["admin"]]

        if share_delete_list:
            TassadarClient().delete_share(share_delete_list)

    admin_user_ids = []
    assigned_rule = MetaHelper().Rule_user_group().get_revert_rule_owner(sub_id)
    for i in assigned_rule:
        # todo:不知道为啥，先把ws——干掉
        if not i["owner"].startswith('ws_'):
            admin_user_ids.append(i["owner"])

    if admin_user_ids:

        res = OverlordCli().check_management(access_token, sub_id, admin_user_ids)
        check_res = res
        rule_delete_admins = [i["admin"] for i in check_res if i["relation"] == 0]
        rule_delete = [i["rule_id"] for i in assigned_rule if i["owner"] in rule_delete_admins]

        if rule_delete:
            MetaHelper().Rule_user_group().delete({"rule_id": rule_delete, "user_group_id": sub_id, "type": 1})

    return True


def check_resource_dependence(admin_user_id, group_id_list, user_id_list):
    if not admin_user_id:
        raise Exception("admin_user_id should not be null or empty")

    if not (group_id_list and user_id_list):
        return True

    # 检查工作表分配依赖
    res = TassadarClient().list_share_of_my(admin_user_id)
    my_share_list = res
    for share in my_share_list:
        if share["to_user"] and share["to_user"] in user_id_list:
            return False

        if share["to_group"] and share["to_group"] in group_id_list:
            return False

    user_group_ids = [i["user_group_id"] for i in
                      MetaHelper().Rule_user_group().get_created_rule_assigned_user_group(admin_user_id)]

    for user_group_id in user_group_ids:
        if user_group_id and user_group_id in user_id_list:
            return False

        if user_group_id and user_group_id in group_id_list:
            return False

    return True
