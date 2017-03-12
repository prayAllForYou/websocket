#!/usr/bin/python
# encoding:utf-8
from common.fe.query.fields import granularity_factory
from common.fe.query.fields import aggr_date_granularity


class AggrDateFactory(granularity_factory.GranularityFactory):
    def get_year(self, fid, config=None):
        return aggr_date_granularity.AggrDateYear(fid)

    def get_month(self, fid, config=None):
        return aggr_date_granularity.AggrDateMonth(fid)

    def get_day(self, fid, config=None):
        return aggr_date_granularity.AggrDateDay(fid)

    def get_hour(self, fid, config=None):
        return aggr_date_granularity.AggrDateHour(fid)

    def get_minute(self, fid, config=None):
        return aggr_date_granularity.AggrDateMinute(fid)

    def get_second(self, fid, config=None):
        return aggr_date_granularity.AggrDateSecond(fid)
