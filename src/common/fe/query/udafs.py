#!/usr/bin/python
# -*- coding: utf-8 -*-
supported_udafs = [
    'max_date',
    'min_date',
    'array_intersect',
    'array_union',
    'collect_set',
    'bol',
    'ltd'
]

def has_udaf_in_formula(formula):
    if not formula:
        return False
    for udaf in supported_udafs:
        if udaf.lower() in formula.lower():
            return udaf.upper()
    return False
