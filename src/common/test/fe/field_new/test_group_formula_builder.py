#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
from common.fe.field_new.group_formula_builder import GroupFormulaBuilder


class TestGroupFormulaBuilder(unittest.TestCase):
    def test_number_custom(self):
        param = {
            "type": "group",
            "name": "新字段名称",
            "fid": "f1",
            "data_type": "number",
            "info": {
                "type": "custom",
                "groups": [
                    {
                        "name": "Excellent",
                        "range": [90, None],
                        "boundary": [1, 0]
                    },
                    {
                        "name": "Good",
                        "range": [80, 90],
                        "boundary": [1, 0]
                    },
                    {
                        "name": "Normal",
                        "range": [60, 80],
                        "boundary": [1, 0]
                    }
                ],
                "default": "Fail"
            }
        }
        builder = GroupFormulaBuilder(param)
        expect = "(CASE WHEN f1>=90 THEN 'Excellent' " \
                 "WHEN (80<=f1 AND f1<90) THEN 'Good' " \
                 "WHEN (60<=f1 AND f1<80) THEN 'Normal' " \
                 "ELSE 'Fail' END)"
        self.assertEqual(expect, builder.build())

    def test_number_custom_empty_interval(self):
        param = {
            "type": "group",
            "name": "新字段名称",
            "fid": "f1",
            "data_type": "number",
            "info": {
                "type": "custom",
                "groups": [
                    {
                        "name": "Excellent",
                        "range": [None, None],
                        "boundary": [1, 0]
                    },
                ],
                "default": "Fail"
            }
        }
        builder = GroupFormulaBuilder(param)
        self.assertEqual("", builder.build())

    def test_number_fixed(self):
        param = {
            "type": "",
            "name": "新字段名称",
            "fid": "f1",
            "data_type": "number",
            "info": {
                "type": "fixed",
                "step": 50,
                "range": [0, 100]
            }
        }
        builder = GroupFormulaBuilder(param)
        expect = 'FIXED_NUMBER_GROUP(f1, 0, 100, 50, "zh")'
        self.assertEqual(expect, builder.build())

    def test_number_fixed_1(self):
        param = {
            "type": "",
            "name": "新字段名称",
            "fid": "f1",
            "data_type": "number",
            "info": {
                "type": "fixed",
                "step": 40,
                "range": [0, 100]
            }
        }
        builder = GroupFormulaBuilder(param)
        expect = 'FIXED_NUMBER_GROUP(f1, 0, 100, 40, "zh")'
        self.assertEqual(expect, builder.build())

    def test_string_condition(self):
        param = {
            "type": "group",
            "name": "字段名",
            "fid": "f1",
            "data_type": "string",
            "info": {
                "groups": [
                    {
                        "name": "Group1",
                        "conditions": [
                            {
                                "operator": 0,
                                "value": "foo"
                            },
                            {
                                "operator": 0,
                                "value": "guy"
                            }

                        ]
                    },
                    {
                        "name": "Group2",
                        "conditions": [
                            {
                                "operator": 1,
                                "value": "bar"
                            }
                        ]
                    },
                    {
                        "name": "Group3",
                        "conditions": [
                            {
                                "operator": 2,
                                "value": "baz"
                            }
                        ]
                    }],
                "default": "Oops"
            }
        }
        builder = GroupFormulaBuilder(param)
        expect = "(CASE WHEN (f1='foo' OR f1='guy') THEN 'Group1' " \
                 "WHEN (INSTRING(f1,'bar')!=0) THEN 'Group2' " \
                 "WHEN (INSTRING(f1,'baz')=0) THEN 'Group3' " \
                 "ELSE 'Oops' END)"
        self.assertEqual(expect, builder.build())

    def test_string_item(self):
        param = {
            "type": "group",
            "name": "字段名",
            "fid": "f1",
            "data_type": "string",
            "info": {
                "type": "item",
                "groups": [
                    {
                        "name": "Balls",
                        "conditions": ["Football", "Basketball", "Volleyball"]
                    },
                    {
                        "name": "Oxygen Sports",
                        "conditions": ["Running", "Swimming"]
                    },
                ],
                "default": "Oops"
            }
        }
        builder = GroupFormulaBuilder(param)
        expect = "(CASE WHEN (ARRAY_CONTAINS(ARRAY('Football', 'Basketball', 'Volleyball'), f1)) THEN 'Balls' " \
                 "WHEN (ARRAY_CONTAINS(ARRAY('Running', 'Swimming'), f1)) THEN 'Oxygen Sports' " \
                 "ELSE 'Oops' END)"
        self.assertEqual(expect, builder.build())

    def test_string_expression(self):
        param = {
            "type": "group",
            "name": "字段名",
            "fid": "f1",
            "data_type": "string",
            "info": {
                "type": "expression",
                "groups": [
                    {
                        "name": "1or2",
                        "expression": "[_field_id_] = '1' or [_field_id_] = '2'"
                    },
                    {
                        "name": "3or4",
                        "expression": "[_field_id_] = '3' or [_field_id_] = '4'"
                    },
                ],
                "default": "Oops"
            }
        }
        builder = GroupFormulaBuilder(param)
        expect = "(CASE WHEN (f1 = '1' or f1 = '2') THEN '1or2' " \
                 "WHEN (f1 = '3' or f1 = '4') THEN '3or4' " \
                 "ELSE 'Oops' END)"
        self.assertEqual(expect, builder.build())

    def test_number_expression(self):
        param = {
            "type": "group",
            "name": "字段名",
            "fid": "f1",
            "data_type": "number",
            "info": {
                "type": "expression",
                "groups": [
                    {
                        "name": "Natural Number",
                        "expression": "[_field_id_] >= 0"
                    },
                    {
                        "name": "Negative Number",
                        "expression": "[_field_id_] < 0"
                    },
                ],
                "default": "Oops"
            }
        }
        builder = GroupFormulaBuilder(param)
        expect = "(CASE WHEN (f1 >= 0) THEN 'Natural Number' " \
                 "WHEN (f1 < 0) THEN 'Negative Number' " \
                 "ELSE 'Oops' END)"
        self.assertEqual(expect, builder.build())

    def test_date_expression(self):
        param = {
            "type": "group",
            "name": "字段名",
            "fid": "f1",
            "data_type": "date",
            "info": {
                "type": "expression",
                "groups": [
                    {
                        "name": "Big",
                        "expression": "ARRAY_CONTAINS(ARRAY(1,3,5,7,8,10,12), MONTH([_field_id_]))"
                    },
                    {
                        "name": "Small",
                        "expression": "ARRAY_CONTAINS(ARRAY(4,6,9,11), MONTH([_field_id_]))"
                    },
                ],
                "default": "February"
            }
        }
        builder = GroupFormulaBuilder(param)
        expect = "(CASE WHEN (ARRAY_CONTAINS(ARRAY(1,3,5,7,8,10,12), MONTH(f1))) THEN 'Big' " \
                 "WHEN (ARRAY_CONTAINS(ARRAY(4,6,9,11), MONTH(f1))) THEN 'Small' " \
                 "ELSE 'February' END)"
        self.assertEqual(expect, builder.build())

    def test_expression_empty(self):
        param = {
            "type": "group",
            "name": "字段名",
            "fid": "f1",
            "data_type": "date",
            "info": {
                "type": "expression",
                "groups": [
                    {
                        "name": "Big",
                        "expression": ""
                    },
                    {
                        "name": "Small",
                        "expression": ""
                    },
                ],
                "default": "February"
            }
        }
        builder = GroupFormulaBuilder(param)
        self.assertEqual("", builder.build())

    def test_expression_constant(self):
        param = {
            "type": "group",
            "name": "字段名",
            "fid": "f1",
            "data_type": "date",
            "info": {
                "type": "expression",
                "groups": [
                    {
                        "name": "Zero",
                        "expression": 0
                    },
                    {
                        "name": "One",
                        "expression": 1
                    },
                    {
                        "name": "Invalid",
                        "items": ""
                    }
                ],
                "default_name": "Oops"
            }
        }
        builder = GroupFormulaBuilder(param)
        expect = "(CASE WHEN (0) THEN 'Zero' " \
                 "WHEN (1) THEN 'One' " \
                 "ELSE 'Oops' END)"
        self.assertEqual(expect, builder.build())

    def test_year_fixed(self):
        param = {
            "type": "group",
            "name": "字段名",
            "fid": "f1",
            "data_type": "date",
            "info": {
                "type": "fixed",
                "aggregator": "year",
                "start": ["2012"],
                "step": 2
            }
        }
        builder = GroupFormulaBuilder(param)
        self.assertEqual('FIXED_DATE_GROUP(f1, "2012", 2, "year", "zh")', builder.build())

    def test_quarter_fixed(self):
        param = {
            "type": "group",
            "name": "字段名",
            "fid": "f1",
            "data_type": "date",
            "info": {
                "type": "fixed",
                "aggregator": "quarter",
                "start": ["2012", "3"],
                "step": 2
            }
        }
        builder = GroupFormulaBuilder(param)
        self.assertEqual('FIXED_DATE_GROUP(f1, "2012,3", 2, "quarter", "zh")', builder.build())

    def test_month_fixed(self):
        param = {
            "type": "group",
            "name": "字段名",
            "fid": "f1",
            "data_type": "date",
            "info": {
                "type": "fixed",
                "aggregator": "month",
                "start": ["2012", "3"],
                "step": 2
            }
        }
        builder = GroupFormulaBuilder(param)
        self.assertEqual('FIXED_DATE_GROUP(f1, "2012,3", 2, "month", "zh")', builder.build())

    def test_week_fixed(self):
        param = {
            "type": "group",
            "name": "字段名",
            "fid": "f1",
            "data_type": "date",
            "info": {
                "type": "fixed",
                "aggregator": "week",
                "start": ["2012", "8"],
                "step": 2
            }
        }
        builder = GroupFormulaBuilder(param)
        self.assertEqual('FIXED_DATE_GROUP(f1, "2012,8", 2, "week", "zh")', builder.build())

    def test_day_fixed(self):
        param = {
            "type": "group",
            "name": "字段名",
            "fid": "f1",
            "data_type": "date",
            "info": {
                "type": "fixed",
                "aggregator": "day",
                "start": ["2012-01-01"],
                "step": 2
            }
        }
        builder = GroupFormulaBuilder(param)
        self.assertEqual('FIXED_DATE_GROUP(f1, "2012-01-01", 2, "day", "zh")', builder.build())

    def test_date_fixed_real_time(self):
        param = {
            "type": "group",
            "name": "字段名",
            "fid": "f1",
            "data_type": "date",
            "info": {
                "type": "fixed_real_time",
                "aggregator": "year",
                "start": ["2012"],
                "end": ["2016"],
                "step": 2
            }
        }
        builder = GroupFormulaBuilder(param)
        result = builder.build().lower()
        self.assertTrue(result != 'fixed_real_time')
        self.assertTrue("case" in result)
        self.assertTrue("when" in result)
        self.assertTrue("then" in result)
        self.assertTrue("else" in result)
        self.assertTrue("end" in result)
