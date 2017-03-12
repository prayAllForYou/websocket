#!/usr/bin/python
# encoding:utf-8
import date_granularity


class AggrDateYear(date_granularity.Year):
    def get_select_field_str(self):
        return 'unify_date(year(%s))' % self.fid


class AggrDateMonth(date_granularity.Month):
    def get_select_field_str(self):
        return 'unify_date(concat(year(%s), " ", month(%s)))' % (self.fid, self.fid)


class AggrDateDay(date_granularity.Day):
    def get_select_field_str(self):
        return 'unify_date(concat(year(%s), " ", month(%s), " ", day(%s)))' % (self.fid, self.fid, self.fid)


class AggrDateHour(date_granularity.Hour):
    def get_select_field_str(self):
        return 'unify_date(concat(year(%s), " ", month(%s), " ", day(%s), " ", hour(%s)))' % (
            self.fid, self.fid, self.fid, self.fid
        )


class AggrDateMinute(date_granularity.Minute):
    def get_select_field_str(self):
        return 'unify_date(concat(year(%s), " ", month(%s), " ", day(%s), " ", hour(%s), " ", minute(%s)))' % (
            self.fid, self.fid, self.fid, self.fid, self.fid
        )


class AggrDateSecond(date_granularity.Second):
    def get_select_field_str(self):
        return 'printf("%%02d-%%02d-%%02d %%02d:%%02d:%%02d", year(%s), month(%s), day(%s), hour(%s), minute(%s), second(%s))' % (
            self.fid, self.fid, self.fid, self.fid, self.fid, self.fid
        )
