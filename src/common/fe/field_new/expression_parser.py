#!/usr/bin/python
# -*- coding: utf-8 -*-


class ExpressionParser(object):
    FIELD_VARIABLE = "[_field_id_]"

    @staticmethod
    def expression_replace(expression, formula):
        # 筛选器表达式， 把表达式中的变量替换为字段id
        if not expression or not formula:
            return expression
        return "(%s)" % expression.replace(ExpressionParser.FIELD_VARIABLE, formula)
