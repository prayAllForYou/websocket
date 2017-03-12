#!/usr/bin/python
# -*- coding: utf-8 -*-

def ignore_carriage_return(str):
    if not str:
        return str
    return str.replace('\r', "").replace('\n', "").replace("\r\n", "")
