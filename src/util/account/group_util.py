#!/usr/bin/python
# -*- coding: utf-8 -*-

from util.account.overlord_cli import OverlordCli

ungroup_id = "00000000000000000000000000000000"

def group_list(token):
    res = []
    if not token:
        return res

    req_res = OverlordCli().group_list(token, detail='True')
    return req_res

def ungroup_user(token):
    res = []
    if not token:
        return res

    req_res = OverlordCli().group_list(token, "true")
    for i in req_res:
        if ungroup_id == i.get('group_id'):
            for j in i['user_list']:
                res.append({'userid':j['user_id'],'name':j['name'],'is_admin':j['is_admin']})
    return res

def get_user_by_groups(group_ids):
    res = {}
    if not group_ids:
        return {}

    req_res = OverlordCli().group_user(group_ids)
    return req_res
