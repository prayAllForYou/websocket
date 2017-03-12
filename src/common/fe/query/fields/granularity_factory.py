#!/usr/bin/python
# encoding:utf-8
import date_granularity


class UnsupportedGranularityError(Exception):
    pass


class GranularityFactory(object):
    def __init__(self, config=None):
        self.config = config

    def create_granularity(self, fid, g_type):
        if g_type == "year":
            return self.get_year(fid, self.config)
        elif g_type == "month":
            return self.get_month(fid, self.config)
        elif g_type == "day":
            return self.get_day(fid, self.config)
        elif g_type == "week":
            return self.get_week(fid, self.config)
        elif g_type == "quarter":
            return self.get_quarter(fid, self.config)
        elif g_type == "hour":
            return self.get_hour(fid, self.config)
        elif g_type == "minute":
            return self.get_minute(fid, self.config)
        elif g_type == "second":
            return self.get_second(fid, self.config)
        else:
            raise UnsupportedGranularityError("Unknown granularity %s" % g_type)

    def get_year(self, fid, config=None):
        return date_granularity.Year(fid, config=config)

    def get_month(self, fid, config=None):
        return date_granularity.Month(fid, config=config)

    def get_quarter(self, fid, config=None):
        return date_granularity.Quarter(fid, config=config)

    def get_week(self, fid, config=None):
        return date_granularity.Week(fid, config=config)

    def get_day(self, fid, config=None):
        return date_granularity.Day(fid, config=config)

    def get_hour(self, fid, config=None):
        return date_granularity.Hour(fid, config=config)

    def get_minute(self, fid, config=None):
        return date_granularity.Minute(fid, config=config)

    def get_second(self, fid, config=None):
        return date_granularity.Second(fid, config=config)
