#!/usr/bin/python
# -*- coding: utf-8 -*-
import unittest
from common.fe.query.fields.aggr_date_factory import AggrDateFactory


class TestAggrDateGranularity(unittest.TestCase):
    def setUp(self):
        self.factory = AggrDateFactory()

    def test_aggr_date_year(self):
        g = self.factory.create_granularity('f0', 'year')
        self.assertEqual("unify_date(year(f0))", g.get_select_field_str())
        self.assertEqual("year(f0)", g.get_group_str())

    def test_aggr_date_quarter(self):
        g = self.factory.create_granularity('f0', 'quarter')
        self.assertEqual("udf_quarter(f0)", g.get_select_field_str())
        self.assertEqual("udf_quarter(f0)", g.get_group_str())

    def test_aggr_date_month(self):
        g = self.factory.create_granularity('f0', 'month')
        self.assertEqual('unify_date(concat(year(f0), " ", month(f0)))', g.get_select_field_str())
        self.assertEqual("year(f0), month(f0)", g.get_group_str())

    def test_aggr_date_week(self):
        g = self.factory.create_granularity('f0', 'week')
        self.assertEqual("udf_week(f0, 1)", g.get_select_field_str())
        self.assertEqual("udf_week(f0, 1)", g.get_group_str())

    def test_aggr_date_day(self):
        g = self.factory.create_granularity('f0', 'day')
        self.assertEqual('unify_date(concat(year(f0), " ", month(f0), " ", day(f0)))', g.get_select_field_str())
        self.assertEqual("year(f0), month(f0), day(f0)", g.get_group_str())

    def test_aggr_date_hour(self):
        g = self.factory.create_granularity('f0', 'hour')
        self.assertEqual(
            'unify_date(concat(year(f0), " ", month(f0), " ", day(f0), " ", hour(f0)))',
            g.get_select_field_str()
        )
        self.assertEqual("year(f0), month(f0), day(f0), hour(f0)", g.get_group_str())

    def test_aggr_date_minute(self):
        g = self.factory.create_granularity('f0', 'minute')
        self.assertEqual(
            'unify_date(concat(year(f0), " ", month(f0), " ", day(f0), " ", hour(f0), " ", minute(f0)))',
            g.get_select_field_str()
        )
        self.assertEqual("year(f0), month(f0), day(f0), hour(f0), minute(f0)", g.get_group_str())

    def test_aggr_date_second(self):
        g = self.factory.create_granularity('f0', 'second')
        self.assertEqual(
                'printf("%02d-%02d-%02d %02d:%02d:%02d", year(f0), month(f0), day(f0), hour(f0), minute(f0), second(f0))',
                g.get_select_field_str()
        )
        self.assertEqual(
                'f0',
                g.get_group_str()
        )

