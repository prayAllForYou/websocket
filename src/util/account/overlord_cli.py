#!/usr/bin/python
# -*- coding: utf-8 -*-

from common.overlord.client import OverlordClient

class OverlordCli(OverlordClient):

    def __init__(self, ip):
        super(OverlordCli, self).__init__(
            ip=ip
        )

if __name__ == '__main__':
    pass
