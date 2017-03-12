#!/usr/bin/python
# encoding:utf-8


class Granularity(object):
    def __init__(self, fid, config=None):
        self.fid = fid
        self._config = config

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, value):
        self._config = value

    def get_select_field_str(self):
        if self.config:
            return self._get_select_by_config()
        else:
            return self._get_select_by_default()

    def _get_select_by_config(self):
        return self.fid

    def _get_select_by_default(self):
        return self.fid

    def get_group_str(self):
        if self.config:
            return self._get_group_by_config()
        else:
            return self._get_group_by_default()

    def _get_group_by_config(self):
        return self._get_select_by_config()

    def _get_group_by_default(self):
        return self.fid

    def __str__(self):
        return "base granularity"


class Year(Granularity):
    def _get_select_by_config(self):
        start_month = self.config['month']
        start_day = self.config['day']
        return "udf_year(%s, %s, %s)" % (self.fid, start_month, start_day)

    def _get_select_by_default(self):
        return 'concat(year(%s), "-01-01 00:00:00")' % self.fid

    def _get_group_by_default(self):
        return "year(%s)" % self.fid

    def __str__(self):
        return "year"


class Month(Granularity):
    def _get_select_by_config(self):
        start_day = self.config['day']
        return 'udf_month(%s, %s)' % (self.fid, start_day)

    def _get_select_by_default(self):
        return 'concat(year(%s), "-", month(%s), "-01 00:00:00")' % (self.fid, self.fid)

    def _get_group_by_default(self):
        return "year(%s), month(%s)" % (self.fid, self.fid)

    def __str__(self):
        return "month"


class Day(Granularity):
    def _get_select_by_config(self):
        return 'udf_day(%s)' % self.fid

    def _get_select_by_default(self):
        return 'concat(year(%s), "-", month(%s), "-", day(%s), " 00:00:00")' % (self.fid, self.fid, self.fid)

    def _get_group_by_default(self):
        return "year(%s), month(%s), day(%s)" % (self.fid, self.fid, self.fid)

    def __str__(self):
        return "day"


class Quarter(Granularity):
    def get_select_field_str(self):
        return 'udf_quarter(%s)' % self.fid

    def get_group_str(self):
        return self.get_select_field_str()

    def __str__(self):
        return "quarter"


class Week(Granularity):
    def get_select_field_str(self):
        start_day = 1
        if self.config:
            start_day = self.config['day_of_week']
        return "udf_week(%s, %s)" % (self.fid, start_day)

    def get_group_str(self):
        return self.get_select_field_str()

    def get_cmp_str(self):
        return 'udf_week(%s, 1)' % self.fid

    def __str__(self):
        return "week"


class Hour(Granularity):
    def _get_select_by_config(self):
        return 'udf_hour(%s)' % self.fid

    def _get_select_by_default(self):
        return 'concat(year(%s), "-", month(%s), "-", day(%s), " ", hour(%s), ":00:00")' % (
            self.fid, self.fid, self.fid, self.fid
        )

    def _get_group_by_default(self):
        return "year(%s), month(%s), day(%s), hour(%s)" % (self.fid, self.fid, self.fid, self.fid)

    def __str__(self):
        return "hour"


class Minute(Granularity):
    def _get_select_by_config(self):
        return 'udf_minute(%s)' % self.fid

    def _get_select_by_default(self):
        return 'concat(year(%s), "-", month(%s), "-", day(%s), " ", hour(%s), ":", minute(%s), ":00")' % (
            self.fid, self.fid, self.fid, self.fid, self.fid
        )

    def _get_group_by_default(self):
        return "year(%s), month(%s), day(%s), hour(%s), minute(%s)" % (self.fid, self.fid, self.fid, self.fid, self.fid)

    def __str__(self):
        return "minute"


class Second(Granularity):
    def _get_select_by_default(self):
        return 'concat(year(%s), "-", month(%s), "-", day(%s), " ", hour(%s), ":", minute(%s), ":", second(%s))' % (
            self.fid, self.fid, self.fid, self.fid, self.fid, self.fid
        )

    def __str__(self):
        return "second"
