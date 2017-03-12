#!/usr/bin/python
# -*- coding: utf-8 -*-


class FormulaBuilder(object):
    def __init__(self, param):
        self.param = param
        self._name = param.get('name', '')
        self._fid = param.get('fid', '')
        self._data_type = param.get('data_type', '')

    def build(self):
        raise NotImplementedError('Exception raised, FormulaBuilder is supposed to be an interface / abstract class!')

    @property
    def data_type(self):
        return self._data_type

    @data_type.setter
    def data_type(self, value):
        self._data_type = value

    @property
    def fid(self):
        return self._fid

    @fid.setter
    def fid(self, value):
        self._fid = value
